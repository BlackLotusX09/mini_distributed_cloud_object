"""Controller (Day 3): worker registry, liveness monitor, placement test endpoint.

Upload/download data paths come later.
"""
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from typing import List, Optional, Tuple

from fastapi import FastAPI, Request

import admin_api
import db
import settings as settings_mod
from errors import ApiError, install_handlers
from lifecycle import cleanup_stale_pending
from objects_api import router as objects_router
from obs import RequestIdMiddleware, configure as configure_logging
from placement import choose_workers
from reconciliation import reconcile_worker
from repair import repair_loop
from scrubber import scrub_cycle, scrub_loop
from worker_client import WorkerClient

configure_logging("controller", os.environ.get("NODE_ID", "controller"))
log = logging.getLogger("controller")


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
            log.info("%s ALIVE -> DEAD (no heartbeat for %ds)", w["worker_id"], int(age))
            transitions.append((w["worker_id"], age))
    return transitions


async def _run_janitor(client: WorkerClient, cfg: settings_mod.Settings):
    """Startup janitor: roll back objects stuck in PENDING past the TTL."""
    try:
        await cleanup_stale_pending(client, cfg.pending_object_ttl_seconds)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("startup janitor failed")


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
def create_app(cfg: Optional[settings_mod.Settings] = None, worker_transport=None) -> FastAPI:
    """`worker_transport`: optional httpx transport (tests inject fake workers here)."""
    cfg = cfg or settings_mod.load()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await asyncio.to_thread(db.init_db, cfg.db_path)
        log.info(
            "controller starting: db=%s heartbeat_timeout=%ss check_interval=%ss R=%d chunk_size=%d",
            cfg.db_path, cfg.heartbeat_timeout, cfg.liveness_check_interval,
            cfg.replication_factor, cfg.chunk_size,
        )
        client = WorkerClient(
            timeout=cfg.upload_worker_timeout, transport=worker_transport,
            read_timeout=cfg.worker_read_timeout, delete_timeout=cfg.worker_delete_timeout,
            scrub_timeout=cfg.worker_scrub_timeout,
        )
        await client.start()
        app.state.worker_client = client
        app.state.trigger_repair_event = asyncio.Event()
        app.state.repair_lock = asyncio.Lock()
        app.state.scrub_lock = asyncio.Lock()
        app.state.reconcile_lock = asyncio.Lock()
        app.state.repair_attempts = {}
        app.state.bg_tasks = set()

        monitor = asyncio.create_task(liveness_monitor(cfg), name="liveness-monitor")
        janitor = asyncio.create_task(_run_janitor(client, cfg), name="pending-janitor")
        repair_task = asyncio.create_task(
            repair_loop(
                cfg, client, app.state.trigger_repair_event,
                app.state.repair_lock, app.state.repair_attempts,
            ),
            name="repair-loop",
        )
        scrub_task = asyncio.create_task(
            scrub_loop(
                cfg, client, app.state.trigger_repair_event, app.state.scrub_lock,
            ),
            name="scrub-loop",
        )
        app.state.janitor_task = janitor
        app.state.repair_task = repair_task
        app.state.scrub_task = scrub_task
        try:
            yield
        finally:
            all_tasks = [monitor, janitor, repair_task, scrub_task, *list(app.state.bg_tasks)]
            for task in all_tasks:
                task.cancel()
            for task in all_tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await client.close()

    app = FastAPI(title="Object Store Controller", lifespan=lifespan)
    app.state.cfg = cfg
    app.include_router(objects_router)
    admin_api.install_admin(app)

    install_handlers(app)  # centralized exception -> JSON registry (errors.py)
    app.add_middleware(RequestIdMiddleware)

    def _schedule_reconcile(worker_id: str) -> None:
        async def _run():
            try:
                await reconcile_worker(
                    worker_id,
                    app.state.worker_client,
                    cfg,
                    trigger_repair_event=app.state.trigger_repair_event,
                    reconcile_lock=app.state.reconcile_lock,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("reconciliation failed for worker %s", worker_id)

        task = asyncio.create_task(_run(), name=f"reconcile-{worker_id}")
        app.state.bg_tasks.add(task)
        task.add_done_callback(app.state.bg_tasks.discard)

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
            _schedule_reconcile(worker_id)
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
            _schedule_reconcile(worker_id)
        else:
            log.debug("heartbeat from %s", worker_id, extra={"worker_id": worker_id})
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

    @app.get("/api/v1/admin/replication")
    async def admin_replication():
        def collect():
            with db.connection() as conn:
                status = db.get_chunk_replica_status(conn=conn, replication_factor=cfg.replication_factor)
                workers_summary = db.get_worker_replica_summary(conn=conn)
                scrub = db.get_scrub_status(conn=conn)
                return status, workers_summary, scrub

        status, workers_summary, scrub = await asyncio.to_thread(collect)
        total_chunks = sum(len(v) for v in status.values())
        return {
            "total_chunks": total_chunks,
            "healthy": len(status["healthy"]),
            "under_replicated": len(status["under_replicated"]),
            "unavailable": len(status["unavailable"]),
            "unrecoverable": len(status["unrecoverable"]),
            "over_replicated": len(status["over_replicated"]),
            "chunks": status,
            "workers": [
                {
                    "worker_id": w["worker_id"],
                    "status": w["status"],
                    "valid_replicas": w["valid_replicas"],
                    "corrupt_replicas": w["corrupt_replicas"],
                    "pending_delete_replicas": w["pending_delete_replicas"],
                }
                for w in workers_summary
            ],
            "scrub": scrub,
        }

    @app.get("/api/v1/admin/objects/{object_id}/chunks")
    async def admin_object_chunks(object_id: str):
        def collect():
            with db.connection() as conn:
                obj = db.get_object_row(object_id, conn=conn)
                if obj is None:
                    return None, []
                chunks = db.get_chunks_for_object_ordered(object_id, conn=conn)
                return obj, chunks

        obj, chunks = await asyncio.to_thread(collect)
        if obj is None:
            raise ApiError(404, "OBJECT_NOT_FOUND", f"object '{object_id}' not found")
        return {
            "object_id": obj["object_id"],
            "name": obj["name"],
            "status": obj["status"],
            "chunks": [
                {
                    "index": ch["idx"],
                    "chunk_id": ch["chunk_id"],
                    "size": ch["size"],
                    "checksum": ch["checksum"],
                    "replicas": [
                        {
                            "worker_id": r["worker_id"],
                            "state": r["state"],
                            "worker_status": r["worker_status"],
                        }
                        for r in ch["replicas"]
                    ],
                }
                for ch in chunks
            ],
        }

    @app.post("/api/v1/admin/scrub")
    async def admin_scrub():
        return await scrub_cycle(
            cfg,
            app.state.worker_client,
            trigger_repair_event=app.state.trigger_repair_event,
            scrub_lock=app.state.scrub_lock,
        )

    return app


app = create_app()

