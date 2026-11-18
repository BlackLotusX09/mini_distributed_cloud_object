"""Worker HTTP daemon: a thin transport wrapper around storage.Storage."""
import json
import re
import shutil
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

import config as config_mod
import worker_obs
from heartbeat import HeartbeatDaemon
from storage import (
    MAX_CHUNK_SIZE,
    ChecksumMismatchError,
    ChunkConflictError,
    ChunkCorruptionError,
    ChunkTooLargeError,
    InvalidChunkIDException,
    NotFoundError,
    Storage,
    WriteStatus,
    sha256_hex,
)

CHUNK_PREFIX = "/internal/chunks/"
_WORKER_CHUNKS_RE = re.compile(r"/internal/workers/([^/]*)/chunks")
_DRAIN_LIMIT = 4 * 1024 * 1024  # never read more than this just to discard a body


def make_handler(storage: Storage, worker_id: str, scrub_yield_delay: float = 0.005):
    class Handler(BaseHTTPRequestHandler):
        server_version = "chunk-worker/1"

        # ------------------------------------------------------ responses
        _rid = "background"

        def _send(self, status, body=b"", content_type="application/json", headers=None):
            self.send_response(status)
            self.send_header("X-Request-ID", self._rid)
            if status != 204:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            if body and status != 204:
                self.wfile.write(body)

        def _json(self, status, obj, headers=None):
            self._send(status, json.dumps(obj).encode(), headers=headers)

        def _error(self, status, code, message):
            self._json(status, {"error": code, "message": message})

        def log_message(self, fmt, *args):  # per-request access lines are DEBUG noise
            worker_obs.log("DEBUG", fmt % args, request_id=self._rid, status="access")

        # --------------------------------------------------------- routing
        def _dispatch(self, method):
            raw_rid = (self.headers.get("X-Request-ID") or "").strip()
            self._rid = raw_rid if re.fullmatch(r"[A-Za-z0-9._\-]{1,128}", raw_rid) else "background"
            path = urlsplit(self.path).path
            try:
                if path == "/health":
                    if method == "GET":
                        return self._health()
                    return self._error(405, "METHOD_NOT_ALLOWED", method)
                if path == "/internal/scrub":
                    if method != "POST":
                        return self._error(405, "METHOD_NOT_ALLOWED", method)
                    return self._scrub()
                if path.startswith(CHUNK_PREFIX):
                    chunk_id = unquote(path[len(CHUNK_PREFIX):])
                    handler = {"PUT": self._put, "GET": self._get, "DELETE": self._delete}.get(method)
                    if handler is None:
                        return self._error(405, "METHOD_NOT_ALLOWED", method)
                    return handler(chunk_id)
                m = _WORKER_CHUNKS_RE.fullmatch(path)
                if m:
                    if method != "GET":
                        return self._error(405, "METHOD_NOT_ALLOWED", method)
                    return self._list(unquote(m.group(1)))
                return self._error(404, "NOT_FOUND", f"no route for {path}")
            except InvalidChunkIDException as e:
                self._error(400, "INVALID_CHUNK_ID", str(e))
            except ChunkTooLargeError as e:
                self._error(400, "CHUNK_TOO_LARGE", str(e))
            except ChecksumMismatchError as e:
                self._error(400, "CHECKSUM_MISMATCH", str(e))
            except NotFoundError as e:
                self._error(404, "CHUNK_NOT_FOUND", str(e))
            except ChunkCorruptionError as e:
                worker_obs.log("WARNING", "chunk corrupt on read", request_id=self._rid, error=str(e))
                self._error(409, "CHUNK_CORRUPT", str(e))
            except ChunkConflictError as e:
                self._error(409, "CHUNK_CONFLICT", str(e))
            except OSError as e:
                worker_obs.log("ERROR", "storage I/O error", request_id=self._rid, error=str(e))
                self._error(500, "STORAGE_ERROR", "storage I/O error")
            except Exception as e:  # never leak a traceback to clients
                import traceback
                worker_obs.log("ERROR", "unhandled error", request_id=self._rid,
                               trace=traceback.format_exc())  # server-side only
                self._error(500, "INTERNAL_ERROR", "Internal server error")

        def do_GET(self):
            self._dispatch("GET")

        def do_PUT(self):
            self._dispatch("PUT")

        def do_DELETE(self):
            self._dispatch("DELETE")

        def do_POST(self):
            self._dispatch("POST")

        def do_PATCH(self):
            self._dispatch("PATCH")

        def do_HEAD(self):
            self._dispatch("HEAD")

        def do_OPTIONS(self):
            self._dispatch("OPTIONS")

        # -------------------------------------------------------- handlers
        def _put(self, chunk_id):
            if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
                return self._error(411, "LENGTH_REQUIRED", "Content-Length is required")
            raw = self.headers.get("Content-Length")
            if raw is None:
                return self._error(411, "LENGTH_REQUIRED", "Content-Length is required")
            try:
                length = int(raw)
                if length < 0:
                    raise ValueError
            except ValueError:
                return self._error(400, "INVALID_LENGTH", "bad Content-Length")
            if length > MAX_CHUNK_SIZE:
                self._drain(length)
                raise ChunkTooLargeError(f"chunk exceeds {MAX_CHUNK_SIZE} bytes")
            body = self.rfile.read(length)
            if len(body) != length:
                return self._error(400, "INCOMPLETE_BODY", "body shorter than Content-Length")
            storage.validate_id(chunk_id)
            result = storage.write_chunk(chunk_id, body, self.headers.get("X-Chunk-Checksum"))
            status = 201 if result.status is WriteStatus.CREATED else 200
            self._json(status, {"chunk_id": result.chunk_id, "size": result.size, "checksum": result.checksum})

        def _drain(self, length):
            remaining = min(length, _DRAIN_LIMIT)
            while remaining > 0:
                data = self.rfile.read(min(65536, remaining))
                if not data:
                    break
                remaining -= len(data)

        def _get(self, chunk_id):
            data = storage.read_chunk(chunk_id)  # verified, or raises: no unverified bytes
            self._send(
                200,
                data,
                content_type="application/octet-stream",
                headers={"X-Chunk-Checksum": sha256_hex(data)},
            )

        def _delete(self, chunk_id):
            storage.delete_chunk(chunk_id)
            self._send(204)

        def _list(self, requested_worker_id):
            if requested_worker_id != worker_id:
                return self._error(404, "WORKER_NOT_FOUND", f"this is {worker_id}")
            chunks = [
                {"chunk_id": c.chunk_id, "size": c.size, "checksum": c.checksum}
                for c in storage.list_chunks()
            ]
            self._json(200, {
                "worker_id": worker_id,
                "chunks": chunks,
                "chunk_mtimes": storage.chunk_mtimes(),
            })

        def _scrub(self):
            result = storage.scrub(yield_delay=scrub_yield_delay)
            self._json(200, result)

        def _health(self):
            try:
                free = shutil.disk_usage(storage.data_dir).free
            except OSError:
                free = 0
            self._json(
                200,
                {
                    "worker_id": worker_id,
                    "chunk_count": storage.chunk_count(),
                    "free_bytes": free,
                    "status": "healthy",
                },
            )

    return Handler


def make_server(storage: Storage, worker_id: str, host="0.0.0.0", port=0, scrub_yield_delay: float = 0.005):
    return ThreadingHTTPServer((host, port), make_handler(storage, worker_id, scrub_yield_delay=scrub_yield_delay))


def main():
    try:
        cfg = config_mod.load()
    except config_mod.ConfigError as e:
        print(f"configuration error: {e}", file=sys.stderr, flush=True)
        sys.exit(2)

    worker_obs.set_node_id(cfg.worker_id)
    storage = Storage(cfg.data_dir)
    worker_obs.log("INFO", "worker starting", data_dir=cfg.data_dir,
                   chunks=storage.chunk_count(), recovered_tmp=storage.recovered_tmp_files)
    server = make_server(storage, cfg.worker_id, port=cfg.port, scrub_yield_delay=cfg.scrub_yield_delay)
    daemon = HeartbeatDaemon(cfg, storage)
    daemon.start()

    def shutdown(_signum, _frame):  # docker stop sends SIGTERM
        daemon.stop()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    worker_obs.log("INFO", "listening", port=cfg.port, address=cfg.address)
    server.serve_forever()
    server.server_close()


if __name__ == "__main__":
    main()
