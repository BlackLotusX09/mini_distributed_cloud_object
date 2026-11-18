"""Worker rejoin & on-disk inventory reconciliation (Day 6)."""
import asyncio
import logging
import time
from typing import Optional, Set

import db
from repair import trim_over_replicated
from settings import Settings
from worker_client import WorkerClient, WorkerClientError

log = logging.getLogger("controller.reconciliation")


async def _safe_delete_on_worker(client: WorkerClient, worker: dict, chunk_id: str) -> bool:
    try:
        await client.delete_chunk(worker, chunk_id)
        return True
    except WorkerClientError as e:
        log.debug("delete_chunk failed during reconciliation worker=%s chunk=%s: %s",
                  worker["worker_id"], chunk_id, e)
        return False


async def reconcile_worker(
    worker_id: str,
    client: WorkerClient,
    cfg: Settings,
    trigger_repair_event: Optional[asyncio.Event] = None,
    reconcile_lock: Optional[asyncio.Lock] = None,
    now: Optional[float] = None,
) -> dict:
    """Synchronize a worker's on-disk chunk inventory with the controller database."""
    lock = reconcile_lock or asyncio.Lock()
    async with lock:
        current_now = time.time() if now is None else now
        worker = await asyncio.to_thread(db.get_worker, worker_id)
        if worker is None or worker["status"] != "ALIVE":
            return {"status": "skipped", "reason": "worker_not_alive"}

        # Capture DB replica snapshot BEFORE querying worker inventory so concurrent uploads
        # that write chunks after inventory snapshot are never mistaken for "missing on disk".
        db_reps_snapshot = await asyncio.to_thread(
            db.get_replicas_for_worker_with_object, worker_id
        )

        try:
            inventory = await client.list_worker_chunks(worker)
        except WorkerClientError as e:
            log.debug("worker %s inventory unreachable during reconciliation: %s", worker_id, e)
            return {"status": "skipped", "reason": "inventory_unreachable"}

        summary = {
            "status": "ok",
            "pending_delete_cleared": 0,
            "restored_ok": 0,
            "marked_corrupt": 0,
            "adopted": 0,
            "deleted_redundant": 0,
            "deleted_stale": 0,
            "deleted_orphan": 0,
            "preserved_pending": 0,
            "preserved_orphan_ttl": 0,
            "removed_missing_db_rows": 0,
            "trimmed": 0,
        }
        needs_repair = False

        # 1. Process PENDING_DELETE rows for this worker
        pending_deletes = [r for r in db_reps_snapshot if r["state"] == "PENDING_DELETE"]
        cleared_ids: Set[str] = set()
        for row in pending_deletes:
            cid = row["chunk_id"]
            if await _safe_delete_on_worker(client, worker, cid):
                await asyncio.to_thread(db.delete_replica, cid, worker_id)
                cleared_ids.add(cid)
                summary["pending_delete_cleared"] += 1
                if row["object_status"] == "DELETING":
                    await asyncio.to_thread(db.finalize_partial_purge, row["object_id"], current_now)

        # 2. Reconcile reported on-disk chunks against DB state
        disk_map = {c["chunk_id"]: c for c in inventory if c["chunk_id"] not in cleared_ids}

        for cid, disk_chunk in disk_map.items():
            info = await asyncio.to_thread(db.get_chunk_info, cid)
            if info is not None:
                obj_status = info["object_status"]
                if obj_status == "PENDING":
                    # Active upload in progress: never delete or mark corrupt
                    summary["preserved_pending"] += 1
                    continue
                if obj_status == "DELETING":
                    if await _safe_delete_on_worker(client, worker, cid):
                        await asyncio.to_thread(db.delete_replica, cid, worker_id)
                        await asyncio.to_thread(db.finalize_partial_purge, info["object_id"], current_now)
                        summary["deleted_stale"] += 1
                    continue

                # Object is COMMITTED
                reps = await asyncio.to_thread(db.get_chunk_replicas_detail, cid)
                existing_rep = next((r for r in reps if r["worker_id"] == worker_id), None)

                disk_sum = (disk_chunk.get("checksum") or "").strip().lower()
                db_sum = (info.get("checksum") or "").strip().lower()
                checksum_ok = (not db_sum) or (disk_sum == db_sum)

                if not checksum_ok:
                    if existing_rep is not None:
                        await asyncio.to_thread(db.set_replica_state, cid, worker_id, "CORRUPT")
                        summary["marked_corrupt"] += 1
                        needs_repair = True
                    else:
                        await _safe_delete_on_worker(client, worker, cid)
                        summary["deleted_stale"] += 1
                else:
                    if existing_rep is not None:
                        await asyncio.to_thread(db.set_replica_state, cid, worker_id, "OK")
                        summary["restored_ok"] += 1
                    else:
                        # Unrecorded valid copy on disk: adopt if under-replicated, else delete
                        valid_cnt = sum(
                            1 for r in reps if r["state"] == "OK" and r["worker_status"] == "ALIVE"
                        )
                        if valid_cnt < cfg.replication_factor:
                            await asyncio.to_thread(db.insert_replica_if_absent, cid, worker_id, "OK")
                            summary["adopted"] += 1
                        else:
                            if await _safe_delete_on_worker(client, worker, cid):
                                summary["deleted_redundant"] += 1
            else:
                # Chunk row does not exist in `chunks` table
                prefix_oid = cid.rsplit("_", 1)[0] if "_" in cid else None
                parent_obj = (
                    await asyncio.to_thread(db.get_object_row, prefix_oid)
                    if prefix_oid
                    else None
                )
                if parent_obj is not None:
                    if parent_obj["status"] == "PENDING":
                        summary["preserved_pending"] += 1
                        continue
                    # Belongs to a DELETING or already-committed object without this chunk row
                    if await _safe_delete_on_worker(client, worker, cid):
                        summary["deleted_stale"] += 1
                    continue

                # Completely unknown to DB (orphan or deleted object chunk)
                mtime = disk_chunk.get("mtime")
                if mtime is not None and (current_now - float(mtime)) < cfg.orphan_safety_ttl_seconds:
                    summary["preserved_orphan_ttl"] += 1
                else:
                    if await _safe_delete_on_worker(client, worker, cid):
                        summary["deleted_orphan"] += 1

        # 3. Missing on disk: only evaluate rows that existed in the pre-inventory snapshot
        for rep in db_reps_snapshot:
            cid = rep["chunk_id"]
            if cid in cleared_ids:
                continue
            if cid not in disk_map:
                if rep["object_status"] == "PENDING":
                    continue
                deleted = await asyncio.to_thread(db.delete_replica, cid, worker_id)
                if deleted:
                    summary["removed_missing_db_rows"] += 1
                    if rep["object_status"] == "DELETING":
                        await asyncio.to_thread(db.finalize_partial_purge, rep["object_id"], current_now)
                    else:
                        needs_repair = True

        # 4. Trim any over-replicated chunks (e.g. rejoined worker restored 3rd replica)
        summary["trimmed"] = await trim_over_replicated(client, cfg.replication_factor)

        if needs_repair and trigger_repair_event is not None:
            trigger_repair_event.set()

        return summary
