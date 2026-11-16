"""Object cleanup: upload rollback, startup janitor, best-effort physical deletes."""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

import db
from worker_client import WorkerClient, WorkerClientError

log = logging.getLogger("controller.lifecycle")
DELETE_CONCURRENCY = 16


async def delete_replicas(client: WorkerClient, replicas: List[dict]) -> Tuple[List[tuple], List[tuple]]:
    """Best-effort delete on workers. Returns (confirmed, failed) lists of (chunk_id, worker_id)."""
    sem = asyncio.Semaphore(DELETE_CONCURRENCY)

    async def one(r):
        async with sem:
            try:
                await client.delete_chunk({"worker_id": r["worker_id"], "address": r["address"]}, r["chunk_id"])
                return (r["chunk_id"], r["worker_id"]), True
            except WorkerClientError as e:
                log.warning("delete chunk=%s worker=%s failed: %s", r["chunk_id"], r["worker_id"], e)
                return (r["chunk_id"], r["worker_id"]), False

    results = await asyncio.gather(*(one(r) for r in replicas))
    return [k for k, ok in results if ok], [k for k, ok in results if not ok]


async def purge_object(
    client: WorkerClient,
    object_id: str,
    inflight: Optional[Tuple[str, Dict[str, dict]]] = None,
) -> dict:
    """Roll back an unfinished (PENDING) object.

    1. best-effort delete every recorded replica (and the chunk that was in flight);
    2. drop confirmed replica rows, mark unreachable ones PENDING_DELETE;
    3. if nothing is left, delete chunks/metadata/object rows. If PENDING_DELETE rows remain,
       the object is kept as DELETING (invisible) so the Day-6 reconciler still knows which
       workers hold orphans: deleting the chunk rows would cascade away that record.
    """
    replicas = await asyncio.to_thread(db.get_object_replicas, object_id)
    deleted, failed = await delete_replicas(client, replicas)
    await asyncio.to_thread(db.apply_replica_cleanup, deleted, failed)

    if inflight:  # chunk sent to workers but never recorded in the DB
        chunk_id, workers = inflight
        for w in workers.values():
            try:
                await client.delete_chunk(w, chunk_id)
            except WorkerClientError as e:
                log.warning("orphan chunk=%s may remain on worker=%s: %s", chunk_id, w["worker_id"], e)

    removed = await asyncio.to_thread(db.finalize_partial_purge, object_id, time.time())
    log.info("rolled back object %s: %d replicas deleted, %d pending delete, object %s",
             object_id, len(deleted), len(failed), "removed" if removed else "kept as DELETING")
    return {"deleted": len(deleted), "pending_delete": len(failed), "removed": removed}


async def cleanup_stale_pending(client: WorkerClient, ttl: float, now: Optional[float] = None) -> int:
    """Startup janitor: roll back objects stuck in PENDING for longer than `ttl`."""
    now = time.time() if now is None else now
    stale = await asyncio.to_thread(db.list_stale_pending, now - ttl)
    for object_id in stale:
        try:
            await purge_object(client, object_id)
        except Exception:
            log.exception("janitor failed for object %s", object_id)
    if stale:
        log.warning("janitor cleaned %d stale PENDING object(s)", len(stale))
    return len(stale)

