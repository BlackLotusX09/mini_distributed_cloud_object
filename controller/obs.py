"""Observability: request-id propagation (contextvars) and JSON-lines logging."""
import contextvars
import json
import logging
import os
import re
import time
import uuid

request_id_var: contextvars.ContextVar = contextvars.ContextVar("request_id", default="background")

_RID_OK = re.compile(r"[A-Za-z0-9._\-]{1,128}")
CONTEXT_FIELDS = ("object_id", "chunk_id", "worker_id", "status")


def new_request_id() -> str:
    return str(uuid.uuid4())


def sanitize_request_id(raw):
    """Accept a client-supplied id only if it is header-safe; otherwise mint one."""
    if raw and _RID_OK.fullmatch(raw):
        return raw
    return new_request_id()


class JsonFormatter(logging.Formatter):
    def __init__(self, component: str, node_id: str):
        super().__init__()
        self.component, self.node_id = component, node_id

    def format(self, record: logging.LogRecord) -> str:
        out = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "component": self.component,
            "node_id": self.node_id,
            "request_id": request_id_var.get(),
            "event": record.getMessage(),
            "logger": record.name,
        }
        for key in CONTEXT_FIELDS:
            if hasattr(record, key):
                out[key] = getattr(record, key)
        if record.exc_info:  # server-side only; never returned to clients
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure(component: str = "controller", node_id: str = "controller") -> None:
    """Install JSON logging on the root logger unless a host (pytest) already did."""
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(component, node_id))
    root.addHandler(handler)
    root.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())


class RequestIdMiddleware:
    """Pure-ASGI middleware: bind request id, echo it in the response, convert crashes to JSON 500."""

    def __init__(self, app):
        self.app = app
        self.log = logging.getLogger("controller.http")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        raw = None
        for k, v in scope.get("headers", []):
            if k == b"x-request-id":
                raw = v.decode("latin-1")
                break
        rid = sanitize_request_id(raw)
        token = request_id_var.set(rid)
        started = False

        async def send_wrapper(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                headers = [h for h in message.get("headers", []) if h[0].lower() != b"x-request-id"]
                headers.append((b"x-request-id", rid.encode("ascii")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            self.log.exception("unhandled error")
            if not started:
                body = json.dumps({"error": "INTERNAL_ERROR", "message": "Internal server error"}).encode()
                await send_wrapper({"type": "http.response.start", "status": 500, "headers": [
                    (b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
                await send_wrapper({"type": "http.response.body", "body": body})
            else:
                raise  # mid-stream failure: abort the transfer rather than truncate silently
        finally:
            request_id_var.reset(token)
