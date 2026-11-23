"""Isolated Admin Dashboard API, state-transition event log, and static UI server.

All new backend logic lives in this module; core controller modules remain intact.
State-transition events are recorded asynchronously/safely by wrapping db/repair
entry points idempotently when installed on the FastAPI app.
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

import db
import lifecycle
import ingestion
import repair
from errors import ApiError, error_response
from settings import Settings

log = logging.getLogger("controller.admin")

MAX_EVENTS_RETAINED = 1000

EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       REAL NOT NULL,
    type     TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('INFO','WARN','ERROR')),
    message  TEXT NOT NULL,
    data     TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
"""

_hooks_lock = threading.Lock()
_hooks_installed = False


# ------------------------------------------------------------------ Pydantic OpenAPI Schemas
class EventItem(BaseModel):
    id: int
    ts: float
    type: str
    severity: str
    message: str
    data: Dict[str, Any] = Field(default_factory=dict)


class EventsResponse(BaseModel):
    events: List[EventItem]
    latest_id: int


class OverviewWorker(BaseModel):
    worker_id: str
    name: str
    address: str
    status: str
    last_heartbeat: float
    last_heartbeat_age: float
    dead_since: Optional[float] = None
    chunk_count: int
    valid_replicas: int
    corrupt_replicas: int
    pending_delete_replicas: int
    used_bytes: int


class OverviewReplication(BaseModel):
    total_chunks: int
    healthy: int
    under_replicated: int
    unavailable: int
    corrupt: int
    corrupt_replicas: int
    unrecoverable: int
    over_replicated: int


class OverviewTotals(BaseModel):
    objects: int
    chunks: int
    logical_bytes: int
    raw_bytes: int


class ScrubSummary(BaseModel):
    last_scrub_at: Optional[float] = None
    chunks_checked: int
    corrupt_found: int


class ClusterConfig(BaseModel):
    replication_factor: int
    chunk_size: int
    heartbeat_timeout: float
    liveness_check_interval: float
    repair_grace_period: float
    repair_interval: float
    scrub_interval: float


class OverviewResponse(BaseModel):
    server_time: float
    workers: List[OverviewWorker]
    replication: OverviewReplication
    totals: OverviewTotals
    last_scrub: ScrubSummary
    config: ClusterConfig


# ------------------------------------------------------------------ Config Guards
def is_admin_enabled() -> bool:
    raw = os.environ.get("ADMIN_ENABLED", "true").strip().lower()
    return raw not in ("false", "0", "no", "off")


def _parse_cors_origins() -> List[str]:
    raw = os.environ.get("CORS_ORIGINS", "").strip()
    if not raw:
        return []
    return [o.strip() for o in raw.split(",") if o.strip()]


def resolve_frontend_dist() -> Path:
    env_dir = os.environ.get("FRONTEND_DIST", "").strip()
    if env_dir:
        return Path(env_dir)
    app_ui = Path("/app/ui_dist")
    if app_ui.exists():
        return app_ui
    return Path(__file__).resolve().parent.parent / "frontend" / "dist"


# ------------------------------------------------------------------ Events Persistence
def init_events_table(path: Optional[str] = None) -> None:
    with db.connection(path) as conn:
        conn.executescript(EVENTS_SCHEMA)


def _ensure_events_table(conn) -> None:
    conn.executescript(EVENTS_SCHEMA)


def record_event(
    event_type: str,
    severity: str,
    message: str,
    ts: Optional[float] = None,
    **data: Any,
) -> Optional[int]:
    """Persist a cluster state-transition event and keep the latest 1,000 rows."""
    sev = severity.upper()
    if sev not in ("INFO", "WARN", "ERROR"):
        sev = "INFO"
    when = time.time() if ts is None else float(ts)
    payload = json.dumps(data, separators=(",", ":"), default=str) if data else "{}"

    try:
        with db.connection() as conn, db.transaction(conn):
            _ensure_events_table(conn)
            cur = conn.execute(
                "INSERT INTO events (ts, type, severity, message, data) VALUES (?, ?, ?, ?, ?)",
                (when, event_type, sev, message, payload),
            )
            event_id = int(cur.lastrowid)
            cutoff = event_id - MAX_EVENTS_RETAINED
            if cutoff > 0:
                conn.execute("DELETE FROM events WHERE id <= ?", (cutoff,))
            return event_id
    except Exception:
        log.debug("failed to record event type=%s", event_type, exc_info=True)
        return None


def get_events_since(since: int = 0, limit: int = 100) -> Dict[str, Any]:
    """Return events with id > since in ascending id order plus latest_id."""
    if since < 0:
        raise ApiError(400, "INVALID_INPUT", "'since' must be >= 0")
    if limit < 1 or limit > 500:
        raise ApiError(400, "INVALID_INPUT", "'limit' must be between 1 and 500")

    with db.connection() as conn:
        _ensure_events_table(conn)
        rows = conn.execute(
            "SELECT id, ts, type, severity, message, data "
            "FROM events WHERE id > ? ORDER BY id ASC LIMIT ?",
            (since, limit),
        ).fetchall()
        max_row = conn.execute("SELECT COALESCE(MAX(id), 0) AS latest_id FROM events").fetchone()
        latest_id = int(max_row["latest_id"]) if max_row else 0

    events: List[Dict[str, Any]] = []
    for r in rows:
        try:
            parsed_data = json.loads(r["data"]) if r["data"] else {}
        except ValueError:
            parsed_data = {}
        events.append({
            "id": int(r["id"]),
            "ts": float(r["ts"]),
            "type": r["type"],
            "severity": r["severity"],
            "message": r["message"],
            "data": parsed_data,
        })
    return {"events": events, "latest_id": latest_id}


# ------------------------------------------------------------------ Non-invasive Hooks
def install_event_hooks() -> None:
    """Wrap state-transition functions idempotently without modifying core modules."""
    global _hooks_installed
    with _hooks_lock:
        if _hooks_installed:
            return

        orig_init_db = db.init_db
        orig_upsert_worker = db.upsert_worker
        orig_record_heartbeat = db.record_heartbeat
        orig_mark_dead = db.mark_dead
        orig_mark_alive = db.mark_alive
        orig_finalize_object = db.finalize_object
        orig_begin_delete = db.begin_delete
        orig_set_replica_state = db.set_replica_state
        orig_mark_corrupt_if_exists = db.mark_chunk_replica_corrupt_if_exists
        orig_save_scrub_status = db.save_scrub_status
        orig_purge_object = lifecycle.purge_object
        orig_copy_chunk = repair.copy_chunk

        @functools.wraps(orig_init_db)
        def _wrapped_init_db(path: Optional[str] = None) -> None:
            orig_init_db(path)
            init_events_table(path)

        @functools.wraps(orig_upsert_worker)
        def _wrapped_upsert_worker(worker_id: str, address: str, now: float, conn=None) -> str:
            prev = None
            if conn is None:
                try:
                    prev = db.get_worker(worker_id)
                except Exception:
                    prev = None
            kind = orig_upsert_worker(worker_id, address, now, conn=conn)
            if conn is None:
                if kind == "new":
                    record_event(
                        "worker_registered",
                        "INFO",
                        f"Worker {worker_id} registered at {address}",
                        ts=now,
                        worker_id=worker_id,
                        address=address,
                    )
                elif prev is not None and prev.get("status") == "DEAD":
                    record_event(
                        "worker_alive",
                        "INFO",
                        f"Worker {worker_id} rejoined and transitioned DEAD -> ALIVE",
                        ts=now,
                        worker_id=worker_id,
                        address=address,
                    )
            return kind

        @functools.wraps(orig_record_heartbeat)
        def _wrapped_record_heartbeat(worker_id: str, now: float, conn=None):
            previous = orig_record_heartbeat(worker_id, now, conn=conn)
            if conn is None and previous == "DEAD":
                record_event(
                    "worker_alive",
                    "INFO",
                    f"Worker {worker_id} heartbeat recovered (DEAD -> ALIVE)",
                    ts=now,
                    worker_id=worker_id,
                )
            return previous

        @functools.wraps(orig_mark_dead)
        def _wrapped_mark_dead(worker_id: str, now: float, stale_before: Optional[float] = None, conn=None) -> bool:
            changed = orig_mark_dead(worker_id, now, stale_before=stale_before, conn=conn)
            if conn is None and changed:
                record_event(
                    "worker_dead",
                    "ERROR",
                    f"Worker {worker_id} missed heartbeats and is marked DEAD",
                    ts=now,
                    worker_id=worker_id,
                    dead_since=now,
                )
            return changed

        @functools.wraps(orig_mark_alive)
        def _wrapped_mark_alive(worker_id: str, now: float, conn=None) -> bool:
            changed = orig_mark_alive(worker_id, now, conn=conn)
            if conn is None and changed:
                record_event(
                    "worker_alive",
                    "INFO",
                    f"Worker {worker_id} marked ALIVE",
                    ts=now,
                    worker_id=worker_id,
                )
            return changed

        @functools.wraps(orig_finalize_object)
        def _wrapped_finalize_object(object_id: str, size: int, checksum: str, chunk_count: int,
                                     now: float, conn=None) -> bool:
            ok = orig_finalize_object(object_id, size, checksum, chunk_count, now, conn=conn)
            if conn is None and ok:
                row = db.get_object_row(object_id)
                name = row["name"] if row else object_id
                record_event(
                    "object_committed",
                    "INFO",
                    f"Object '{name}' committed ({size} B, {chunk_count} chunk(s))",
                    ts=now,
                    object_id=object_id,
                    name=name,
                    size=size,
                    chunk_count=chunk_count,
                )
            return ok

        @functools.wraps(orig_begin_delete)
        def _wrapped_begin_delete(object_id: str, now: float, conn=None):
            outcome, replicas = orig_begin_delete(object_id, now, conn=conn)
            if conn is None and outcome == "ok":
                record_event(
                    "object_deleted",
                    "INFO",
                    f"Object {object_id} marked for deletion ({len(replicas)} replica(s))",
                    ts=now,
                    object_id=object_id,
                    replica_count=len(replicas),
                )
            return outcome, replicas

        @functools.wraps(orig_set_replica_state)
        def _wrapped_set_replica_state(chunk_id: str, worker_id: str, state: str, conn=None) -> bool:
            changed = orig_set_replica_state(chunk_id, worker_id, state, conn=conn)
            if conn is None and changed and state == "CORRUPT":
                record_event(
                    "replica_corrupt",
                    "ERROR",
                    f"Replica marked CORRUPT: {chunk_id} on {worker_id}",
                    chunk_id=chunk_id,
                    worker_id=worker_id,
                )
            return changed

        @functools.wraps(orig_mark_corrupt_if_exists)
        def _wrapped_mark_corrupt_if_exists(chunk_id: str, worker_id: str, conn=None) -> bool:
            changed = orig_mark_corrupt_if_exists(chunk_id, worker_id, conn=conn)
            if conn is None and changed:
                record_event(
                    "replica_corrupt",
                    "ERROR",
                    f"Corrupt replica detected: {chunk_id} on {worker_id}",
                    chunk_id=chunk_id,
                    worker_id=worker_id,
                )
            return changed

        @functools.wraps(orig_save_scrub_status)
        def _wrapped_save_scrub_status(last_scrub_at: float, chunks_checked: int,
                                       corrupt_found: int, conn=None) -> None:
            orig_save_scrub_status(last_scrub_at, chunks_checked, corrupt_found, conn=conn)
            if conn is None:
                sev = "WARN" if corrupt_found > 0 else "INFO"
                record_event(
                    "scrub_finished",
                    sev,
                    f"Scrub finished: checked {chunks_checked} chunk(s), {corrupt_found} corrupt found",
                    ts=last_scrub_at,
                    chunks_checked=chunks_checked,
                    corrupt_found=corrupt_found,
                )

        @functools.wraps(orig_purge_object)
        async def _wrapped_purge_object(client, object_id: str, inflight=None) -> dict:
            res = await orig_purge_object(client, object_id, inflight=inflight)
            await asyncio.to_thread(
                record_event,
                "object_rolled_back",
                "WARN",
                f"Upload rolled back for object {object_id}",
                object_id=object_id,
                deleted=res.get("deleted", 0),
                pending_delete=res.get("pending_delete", 0),
                removed=res.get("removed", True),
            )
            return res

        @functools.wraps(orig_copy_chunk)
        async def _wrapped_copy_chunk(chunk_id: str, client, replication_factor: int = 2, rng=repair.random):
            should_log_start = False
            try:
                info = await asyncio.to_thread(db.get_chunk_info, chunk_id)
                if info is not None and info["object_status"] == "COMMITTED":
                    reps = await asyncio.to_thread(db.get_chunk_replicas_detail, chunk_id)
                    valid = [r for r in reps if r["state"] == "OK" and r["worker_status"] == "ALIVE"]
                    if 0 < len(valid) < replication_factor:
                        should_log_start = True
            except Exception:
                pass

            if should_log_start:
                await asyncio.to_thread(
                    record_event,
                    "repair_started",
                    "INFO",
                    f"Starting re-replication for chunk {chunk_id}",
                    chunk_id=chunk_id,
                )

            outcome, target_id = await orig_copy_chunk(chunk_id, client, replication_factor, rng)
            if outcome == "repaired":
                await asyncio.to_thread(
                    record_event,
                    "repair_completed",
                    "INFO",
                    f"Repaired chunk {chunk_id} onto {target_id}",
                    chunk_id=chunk_id,
                    target_worker=target_id,
                )
            elif outcome in ("failed", "source_corrupt", "no_target"):
                await asyncio.to_thread(
                    record_event,
                    "repair_failed",
                    "ERROR",
                    f"Repair failed for chunk {chunk_id}: {outcome}",
                    chunk_id=chunk_id,
                    outcome=outcome,
                )
            return outcome, target_id

        db.init_db = _wrapped_init_db
        db.upsert_worker = _wrapped_upsert_worker
        db.record_heartbeat = _wrapped_record_heartbeat
        db.mark_dead = _wrapped_mark_dead
        db.mark_alive = _wrapped_mark_alive
        db.finalize_object = _wrapped_finalize_object
        db.begin_delete = _wrapped_begin_delete
        db.set_replica_state = _wrapped_set_replica_state
        db.mark_chunk_replica_corrupt_if_exists = _wrapped_mark_corrupt_if_exists
        db.save_scrub_status = _wrapped_save_scrub_status
        lifecycle.purge_object = _wrapped_purge_object
        ingestion.purge_object = _wrapped_purge_object
        repair.copy_chunk = _wrapped_copy_chunk
        _hooks_installed = True


# ------------------------------------------------------------------ Overview Aggregation
def collect_overview(cfg: Settings) -> Dict[str, Any]:
    now = time.time()
    with db.connection() as conn:
        _ensure_events_table(conn)
        workers_raw = db.list_workers(conn=conn)
        rep_summary = {r["worker_id"]: r for r in db.get_worker_replica_summary(conn=conn)}
        counts = db.count_chunks_per_worker(conn=conn)
        used_rows = conn.execute(
            "SELECT r.worker_id, COALESCE(SUM(c.size), 0) AS used_bytes "
            "FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
            "JOIN objects o ON o.object_id = c.object_id "
            "WHERE r.state IN ('OK', 'CORRUPT') AND o.status != 'PENDING' "
            "GROUP BY r.worker_id"
        ).fetchall()
        used_map = {r["worker_id"]: int(r["used_bytes"]) for r in used_rows}

        status = db.get_chunk_replica_status(conn=conn, replication_factor=cfg.replication_factor)
        scrub = db.get_scrub_status(conn=conn)

        obj_row = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(size), 0) AS logical_bytes "
            "FROM objects WHERE status = 'COMMITTED'"
        ).fetchone()
        chunk_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM chunks c JOIN objects o ON o.object_id = c.object_id "
            "WHERE o.status = 'COMMITTED'"
        ).fetchone()
        raw_row = conn.execute(
            "SELECT COALESCE(SUM(c.size), 0) AS raw_bytes "
            "FROM replicas r JOIN chunks c ON c.chunk_id = r.chunk_id "
            "JOIN objects o ON o.object_id = c.object_id "
            "WHERE o.status = 'COMMITTED' AND r.state IN ('OK', 'CORRUPT')"
        ).fetchone()

    workers: List[Dict[str, Any]] = []
    for w in workers_raw:
        wid = w["worker_id"]
        s = rep_summary.get(wid, {})
        workers.append({
            "worker_id": wid,
            "name": wid,
            "address": w["address"],
            "status": w["status"],
            "last_heartbeat": float(w["last_heartbeat"]),
            "last_heartbeat_age": round(max(0.0, now - float(w["last_heartbeat"])), 1),
            "dead_since": float(w["dead_since"]) if w["dead_since"] is not None else None,
            "chunk_count": int(counts.get(wid, 0)),
            "valid_replicas": int(s.get("valid_replicas", 0)),
            "corrupt_replicas": int(s.get("corrupt_replicas", 0)),
            "pending_delete_replicas": int(s.get("pending_delete_replicas", 0)),
            "used_bytes": int(used_map.get(wid, 0)),
        })

    corrupt_chunks = sum(
        1 for bucket in status.values() for ch in bucket if ch.get("corrupt_count", 0) > 0
    )
    corrupt_replicas_total = sum(w["corrupt_replicas"] for w in workers)
    total_chunks = sum(len(v) for v in status.values())

    return {
        "server_time": now,
        "workers": workers,
        "replication": {
            "total_chunks": total_chunks,
            "healthy": len(status["healthy"]),
            "under_replicated": len(status["under_replicated"]),
            "unavailable": len(status["unavailable"]),
            "corrupt": corrupt_chunks,
            "corrupt_replicas": corrupt_replicas_total,
            "unrecoverable": len(status["unrecoverable"]),
            "over_replicated": len(status["over_replicated"]),
        },
        "totals": {
            "objects": int(obj_row["cnt"]) if obj_row else 0,
            "chunks": int(chunk_row["cnt"]) if chunk_row else 0,
            "logical_bytes": int(obj_row["logical_bytes"]) if obj_row else 0,
            "raw_bytes": int(raw_row["raw_bytes"]) if raw_row else 0,
        },
        "last_scrub": {
            "last_scrub_at": scrub.get("last_scrub_at"),
            "chunks_checked": int(scrub.get("chunks_checked", 0)),
            "corrupt_found": int(scrub.get("corrupt_found", 0)),
        },
        "config": {
            "replication_factor": cfg.replication_factor,
            "chunk_size": cfg.chunk_size,
            "heartbeat_timeout": cfg.heartbeat_timeout,
            "liveness_check_interval": cfg.liveness_check_interval,
            "repair_grace_period": cfg.repair_grace_period,
            "repair_interval": cfg.repair_interval,
            "scrub_interval": cfg.scrub_interval,
        },
    }


# ------------------------------------------------------------------ Router & Installation
admin_router = APIRouter()


@admin_router.get("/api/v1/admin/overview", response_model=OverviewResponse)
async def admin_overview(request: Request):
    return await asyncio.to_thread(collect_overview, request.app.state.cfg)


@admin_router.get("/api/v1/admin/events", response_model=EventsResponse)
async def admin_events(
    since: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
):
    return await asyncio.to_thread(get_events_since, since, limit)


def _serve_ui_path(full_path: str = ""):
    dist = resolve_frontend_dist().resolve()
    index_file = dist / "index.html"
    if not index_file.is_file():
        return JSONResponse(
            status_code=404,
            content={"error": "UI_NOT_BUILT", "message": "Frontend static bundle not found at /app/ui_dist"},
        )
    cleaned = full_path.lstrip("/")
    if cleaned:
        candidate = (dist / cleaned).resolve()
        try:
            candidate.relative_to(dist)
        except ValueError:
            return error_response(400, "INVALID_REQUEST", "invalid UI path")
        if candidate.is_file():
            return FileResponse(candidate)
        # Missing asset files (.js/.css/etc.) must 404 instead of falling back to HTML
        if "." in Path(cleaned).name:
            return error_response(404, "NOT_FOUND", f"asset '{cleaned}' not found")
    return FileResponse(index_file)


def install_admin(app: FastAPI) -> None:
    """Attach admin routes, ADMIN_ENABLED guard, optional CORS, UI static mount, and event hooks."""
    install_event_hooks()
    app.include_router(admin_router)

    @app.get("/ui", include_in_schema=False)
    async def ui_root():
        return _serve_ui_path("")

    @app.get("/ui/{full_path:path}", include_in_schema=False)
    async def ui_spa(full_path: str):
        return _serve_ui_path(full_path)

    @app.middleware("http")
    async def admin_enabled_middleware(request: Request, call_next):
        if request.url.path.startswith("/api/v1/admin") and not is_admin_enabled():
            return error_response(403, "ADMIN_DISABLED", "admin endpoints are disabled (ADMIN_ENABLED=false)")
        return await call_next(request)

    origins = _parse_cors_origins()
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
