"""Background repair and over-replication trimming loop (Day 6)."""
import asyncio
import hashlib
import logging
import random
import time
from typing import Dict, List, Optional, Tuple

import db
from placement import InsufficientWorkersError, choose_workers
from settings import Settings
from worker_client import WorkerClient, WorkerClientError, WorkerRejected

log = logging.getLogger("controller.repair")


async def copy_chunk(
    chunk_id: str,
    client: WorkerClient,
    replication_factor: int = 2,
    rng=random,
) -> Tuple[str, Optional[str]]:
    """Copy an under-replicated chunk from a valid source to a distinct ALIVE worker.

    Returns (outcome, target_worker_id) where outcome is one of:
      'repaired', 'already_healthy', 'no_target', 'source_corrupt', 'failed'.
    """
    info = await asyncio.to_thread(db.get_chunk_info, chunk_id)
    if info is None or info["object_status"] != "COMMITTED":
        return "already_healthy", None

    replicas = await asyncio.to_thread(db.get_chunk_replicas_detail, chunk_id)
    valid_sources = [r for r in replicas if r["state"] == "OK" and r["worker_status"] == "ALIVE"]
    if len(valid_sources) >= replication_factor:
        return "already_healthy", None
    if not valid_sources:
        return "source_corrupt", None

    corrupt_replicas = [r for r in replicas if r["state"] == "CORRUPT"]
    excluded = {r["worker_id"] for r in replicas if r["state"] in ("OK", "CORRUPT")}

    alive = await asyncio.to_thread(db.list_alive_with_counts)
    try:
        target_ids = choose_workers(alive, 1, exclude=excluded, rng=rng)
    except InsufficientWorkersError:
        log.warning("cannot repair %s: cannot repair, %d workers alive", chunk_id, len(alive))
        return "no_target", None

    target_id = target_ids[0]
    by_id = {w["worker_id"]: w for w in alive}
    target_worker = by_id[target_id]

    rng.shuffle(valid_sources)
    data: Optional[bytes] = None
    source_id: Optional[str] = None
    expected_checksum = info["checksum"]
    expected_size = info["size"]

    for src in valid_sources:
        src_worker = {"worker_id": src["worker_id"], "address": src["address"]}
        try:
            candidate = await client.get_chunk(src_worker, chunk_id)
        except WorkerRejected as e:
            log.warning("repair source rejected chunk=%s worker=%s (%s); marking CORRUPT",
                        chunk_id, src["worker_id"], e)
            await asyncio.to_thread(db.set_replica_state, chunk_id, src["worker_id"], "CORRUPT")
            continue
        except WorkerClientError as e:
            log.warning("repair source unreachable chunk=%s worker=%s (%s)",
                        chunk_id, src["worker_id"], e)
            continue

        if len(candidate) != expected_size or (
            expected_checksum is not None
            and hashlib.sha256(candidate).hexdigest() != expected_checksum
        ):
            log.error("repair source checksum mismatch chunk=%s worker=%s; marking CORRUPT",
                      chunk_id, src["worker_id"])
            await asyncio.to_thread(db.set_replica_state, chunk_id, src["worker_id"], "CORRUPT")
            continue

        data = candidate
        source_id = src["worker_id"]
        break

    if data is None or source_id is None:
        # Check if all valid sources were condemned to CORRUPT
        refreshed = await asyncio.to_thread(db.get_chunk_replicas_detail, chunk_id)
        if not any(r["state"] == "OK" and r["worker_status"] == "ALIVE" for r in refreshed):
            return "source_corrupt", None
        return "failed", None

    checksum = expected_checksum or hashlib.sha256(data).hexdigest()
    try:
        await client.put_chunk(target_worker, chunk_id, data, checksum)
    except WorkerClientError as e:
        log.warning("repair upload failed chunk=%s target=%s: %s", chunk_id, target_id, e)
        return "failed", None

    # Record new OK replica atomically using composite PK (chunk_id, worker_id)
    await asyncio.to_thread(db.upsert_replica_ok, chunk_id, target_id)
    log.info("repaired chunk %s: source %s -> target %s", chunk_id, source_id, target_id)

    # Clean up any CORRUPT replica copies now that a healthy replacement exists
    for cr in corrupt_replicas:
        if cr["worker_status"] == "ALIVE":
            try:
                await client.delete_chunk({"worker_id": cr["worker_id"], "address": cr["address"]}, chunk_id)
                await asyncio.to_thread(db.delete_replica, chunk_id, cr["worker_id"])
            except WorkerClientError as e:
                log.warning("failed deleting CORRUPT chunk=%s on worker=%s (%s); marking PENDING_DELETE",
                            chunk_id, cr["worker_id"], e)
                await asyncio.to_thread(db.set_replica_state, chunk_id, cr["worker_id"], "PENDING_DELETE")
        else:
            await asyncio.to_thread(db.set_replica_state, chunk_id, cr["worker_id"], "PENDING_DELETE")

    return "repaired", target_id


async def trim_over_replicated(client: WorkerClient, replication_factor: int = 2) -> int:
    """Remove excess valid replicas (valid_count > R), preferring highest-loaded workers."""
    over = await asyncio.to_thread(db.get_over_replicated_chunks, replication_factor)
    trimmed = 0
    for item in over:
        chunk_id = item["chunk_id"]
        replicas = await asyncio.to_thread(db.get_chunk_replicas_detail, chunk_id)
        counts = await asyncio.to_thread(db.count_chunks_per_worker)
        valid = [r for r in replicas if r["state"] == "OK" and r["worker_status"] == "ALIVE"]
        while len(valid) > replication_factor:
            valid.sort(key=lambda r: (counts.get(r["worker_id"], 0), r["worker_id"]), reverse=True)
            victim = valid[0]
            wid = victim["worker_id"]
            try:
                await client.delete_chunk({"worker_id": wid, "address": victim["address"]}, chunk_id)
            except WorkerClientError as e:
                log.warning("trim delete failed chunk=%s worker=%s: %s", chunk_id, wid, e)
                break
            await asyncio.to_thread(db.delete_replica, chunk_id, wid)
            valid.pop(0)
            counts[wid] = max(0, counts.get(wid, 1) - 1)
            trimmed += 1
            log.info("trimmed over-replicated chunk %s from worker %s", chunk_id, wid)
    return trimmed


def _is_eligible(item: dict, replicas: List[dict], now: float, grace_period: float, replication_factor: int) -> bool:
    """Determine if an under-replicated chunk should be repaired right now."""
    has_corrupt = any(r["state"] == "CORRUPT" for r in replicas)
    if has_corrupt:
        return True  # immediate repair for CORRUPT replica
    ok_alive = sum(1 for r in replicas if r["state"] == "OK" and r["worker_status"] == "ALIVE")
    ok_dead = sum(1 for r in replicas if r["state"] == "OK" and r["worker_status"] == "DEAD")
    if ok_alive + ok_dead < replication_factor:
        return True  # missing replica row(s), not merely a DEAD worker -> repair immediately
    dead_since = item.get("dead_since_min")
    if dead_since is None:
        return False
    return (now - dead_since) >= grace_period


async def repair_cycle(
    cfg: Settings,
    client: WorkerClient,
    repair_lock: Optional[asyncio.Lock] = None,
    attempt_counts: Optional[Dict[str, int]] = None,
    now: Optional[float] = None,
) -> dict:
    """Run one bounded, non-overlapping repair and over-replication-trimming pass."""
    lock = repair_lock or asyncio.Lock()
    attempts = attempt_counts if attempt_counts is not None else {}

    async with lock:
        current_now = time.time() if now is None else now
        status = await asyncio.to_thread(db.get_chunk_replica_status, None, cfg.replication_factor)
        under = status["under_replicated"]
        unavailable_cnt = len(status["unavailable"]) + len(status["unrecoverable"])

        eligible: List[str] = []
        skipped_backoff = 0
        for item in under:
            cid = item["chunk_id"]
            if attempts.get(cid, 0) >= cfg.max_chunk_repair_attempts:
                skipped_backoff += 1
                log.warning(
                    "chunk %s exceeded max repair attempts (%d); skipping (backoff)",
                    cid, cfg.max_chunk_repair_attempts,
                )
                continue
            replicas = await asyncio.to_thread(db.get_chunk_replicas_detail, cid)
            if _is_eligible(item, replicas, current_now, cfg.repair_grace_period, cfg.replication_factor):
                eligible.append(cid)
                if len(eligible) >= cfg.max_repairs_per_cycle:
                    break

        repaired = 0
        failed = 0
        if eligible:
            sem = asyncio.Semaphore(cfg.repair_concurrency)

            async def _run_one(cid: str):
                async with sem:
                    return cid, await copy_chunk(cid, client, cfg.replication_factor)

            results = await asyncio.gather(*(_run_one(cid) for cid in eligible))
            for cid, (outcome, _target) in results:
                if outcome == "repaired":
                    attempts.pop(cid, None)
                    repaired += 1
                elif outcome == "already_healthy":
                    attempts.pop(cid, None)
                else:
                    attempts[cid] = attempts.get(cid, 0) + 1
                    failed += 1
                    if attempts[cid] == cfg.max_chunk_repair_attempts:
                        log.error("repair of chunk %s failed after %d attempts; giving up (outcome=%s)",
                                  cid, attempts[cid], outcome, extra={"chunk_id": cid})

        trimmed = await trim_over_replicated(client, cfg.replication_factor)

        if repaired > 0 or failed > 0 or eligible or trimmed > 0 or skipped_backoff > 0:
            log.info(
                "cycle: %d under-replicated, %d repaired, %d failed, %d unavailable",
                len(under), repaired, failed, unavailable_cnt,
            )

        return {
            "under_replicated": len(under),
            "eligible": len(eligible),
            "repaired": repaired,
            "failed": failed,
            "unavailable": unavailable_cnt,
            "trimmed": trimmed,
            "skipped_backoff": skipped_backoff,
        }


async def repair_loop(
    cfg: Settings,
    client: WorkerClient,
    trigger_event: asyncio.Event,
    repair_lock: asyncio.Lock,
    attempt_counts: Dict[str, int],
) -> None:
    """Periodic background repair loop with early wakeup on trigger_event."""
    while True:
        woken_early = False
        try:
            await asyncio.wait_for(trigger_event.wait(), timeout=cfg.repair_interval)
            woken_early = True
        except asyncio.TimeoutError:
            pass

        if woken_early:
            # Brief coalesce delay so multiple concurrent CORRUPT detections batch together
            await asyncio.sleep(min(0.05, cfg.repair_interval))
            trigger_event.clear()

        try:
            await repair_cycle(cfg, client, repair_lock=repair_lock, attempt_counts=attempt_counts)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("repair cycle failed; will retry")
