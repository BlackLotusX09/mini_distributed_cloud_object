"""Controller (Day 3): worker registry, liveness monitor, placement test endpoint.

Upload/download data paths come later.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager, suppress
from typing import List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

import db
import settings as settings_mod
from placement import InsufficientWorkersError, choose_workers

if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("controller")


# ----------------------------------------------------------------- errors
class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


def error_response(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message})


# ------------------------------------------------------------ validation
async def read_json_object(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception:
        raise ApiError(400, "INVALID_REQUEST", "request body must be valid JSON") from None
    if not isinstance(body, dict):
        raise ApiError(400, "INVALID_REQUEST", "request body must be a JSON object")
    return body


def required_str(body: dict, key: str, max_len: int = 256) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(400, "INVALID_REQUEST", f"'{key}' is required and must be a non-empty string")
    value = value.strip()
    if len(value) > max_len:
        raise ApiError(400, "INVALID_REQUEST", f"'{key}' is too long (max {max_len})")
    return value


def optional_nonneg_int(body: dict, key: str) -> Optional[int]:
    if key not in body or body[key] is None:
        return None
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ApiError(400, "INVALID_REQUEST", f"'{key}' must be a non-negative integer")
    return value


def valid_address(address: str) -> str:
    if not address.startswith(("http://", "https://")) or len(address.split("://", 1)[1]) == 0:
        raise ApiError(400, "INVALID_REQUEST", "'address' must be an http(s) URL")
    return address


# --------------------------------------------------------------- liveness
def sweep_dead_workers(now: float, timeout: float) -> List[Tuple[str, float]]:
    """Mark ALIVE workers with no heartbeat for > timeout as DEAD.

    Only the worker row changes; replicas are never touched here.
    Returns [(worker_id, seconds_since_heartbeat)] for each transition.
    """
    transitions = []
    for w in db.list_workers("ALIVE"):
        age = now - w["last_heartbeat"]
        if age > timeout and db.mark_dead(w["worker_id"], now, stale_before=now - timeout):
            log.warning("%s ALIVE -> DEAD (no heartbeat for %ds)", w["worker_id"], int(age))
            transitions.append((w["worker_id"], age))
    return transitions


async def liveness_monitor(cfg: settings_mod.Settings):
    started = time.monotonic()
    while True:
        await asyncio.sleep(cfg.liveness_check_interval)
        try:
            if time.monotonic() - started < cfg.heartbeat_timeout:
                continue  # startup grace: let workers send their next heartbeat first
            await asyncio.to_thread(sweep_dead_workers, time.time(), cfg.heartbeat_timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("liveness check failed; will retry")


# -------------------------------------------------------------------- app
def create_app(cfg: Optional[settings_mod.Settings] = None) -> FastAPI:
    cfg = cfg or settings_mod.load()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(db.init_db, cfg.db_path)
        log.info(
            "controller starting: db=%s heartbeat_timeout=%ss check_interval=%ss R=%d chunk_size=%d",
            cfg.db_path, cfg.heartbeat_timeout, cfg.liveness_check_interval,
            cfg.replication_factor, cfg.chunk_size,
        )
        task = asyncio.create_task(liveness_monitor(cfg), name="liveness-monitor")
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="Object Store Controller", lifespan=lifespan)
    app.state.cfg = cfg

    # ---- error handlers: every error is {"error": CODE, "message": ...}
    @app.exception_handler(ApiError)
    async def _api_error(_req, exc: ApiError):
        return error_response(exc.status, exc.code, exc.message)

    @app.exception_handler(InsufficientWorkersError)
    async def _insufficient(_req, exc: InsufficientWorkersError):
        return error_response(
            503, "INSUFFICIENT_WORKERS",
            f"need {exc.needed} healthy workers, have {exc.available}",
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(_req, exc: RequestValidationError):
        return error_response(400, "INVALID_REQUEST", "malformed request")

    @app.exception_handler(StarletteHTTPException)
    async def _http(_req, exc: StarletteHTTPException):
        codes = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}
        return error_response(exc.status_code, codes.get(exc.status_code, "HTTP_ERROR"), str(exc.detail))

    @app.exception_handler(Exception)
    async def _unexpected(_req, exc: Exception):
        log.exception("unhandled error")
        return error_response(500, "INTERNAL_ERROR", "internal error")

    # ---- endpoints
    @app.get("/health")
    async def health():
        alive = await asyncio.to_thread(db.list_workers, "ALIVE")
        return {"status": "ok", "role": "controller", "active_workers": len(alive)}

    @app.post("/internal/workers/register")
    async def register(request: Request):
        body = await read_json_object(request)
        worker_id = required_str(body, "worker_id", 128)
        address = valid_address(required_str(body, "address", 512))
        optional_nonneg_int(body, "free_bytes")   # validated; not persisted yet
        optional_nonneg_int(body, "chunk_count")
        now = time.time()  # controller clock only; worker timestamps are ignored
        previous = await asyncio.to_thread(db.get_worker, worker_id)
        kind = await asyncio.to_thread(db.upsert_worker, worker_id, address, now)
        if kind == "new":
            log.info("worker %s registered (new) at %s", worker_id, address)
        else:
            was = previous["status"] if previous else "?"
            log.info("worker %s registered (returning, was %s) at %s", worker_id, was, address)
        return {"status": "registered", "heartbeat_interval": cfg.heartbeat_interval}

    @app.post("/internal/workers/heartbeat")
    async def heartbeat(request: Request):
        body = await read_json_object(request)
        worker_id = required_str(body, "worker_id", 128)
        optional_nonneg_int(body, "free_bytes")
        optional_nonneg_int(body, "chunk_count")
        previous = await asyncio.to_thread(db.record_heartbeat, worker_id, time.time())
        if previous is None:
            raise ApiError(404, "UNKNOWN_WORKER", f"worker '{worker_id}' is not registered")
        if previous == "DEAD":
            log.info("worker %s DEAD -> ALIVE", worker_id)
        return {"status": "ok"}

    @app.get("/api/v1/admin/workers")
    async def admin_workers():
        def collect():
            with db.connection() as conn:
                return db.list_workers(conn=conn), db.count_chunks_per_worker(conn=conn)

        workers, counts = await asyncio.to_thread(collect)
        now = time.time()
        return [
            {
                "worker_id": w["worker_id"],
                "address": w["address"],
                "status": w["status"],
                "last_heartbeat_age": round(max(0.0, now - w["last_heartbeat"]), 1),
                "dead_since": w["dead_since"],
                "chunk_count": counts.get(w["worker_id"], 0),
            }
            for w in workers
        ]

    @app.get("/api/v1/admin/placement-test")  # temporary: remove once upload uses placement
    async def placement_test():
        def collect():
            with db.connection() as conn:
                alive = db.list_workers("ALIVE", conn=conn)
                counts = db.count_chunks_per_worker(conn=conn)
            return [
                {"worker_id": w["worker_id"], "status": w["status"],
                 "chunk_count": counts.get(w["worker_id"], 0)}
                for w in alive
            ]

        candidates = await asyncio.to_thread(collect)
        chosen = choose_workers(candidates, cfg.replication_factor)
        return {"replication_factor": cfg.replication_factor, "workers": chosen}

    return app


app = create_app()

