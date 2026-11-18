"""Background two-layer data integrity scrubber (Day 7)."""
import asyncio
import logging
import time
from typing import Optional

import db
from settings import Settings
from worker_client import WorkerClient, WorkerClientError

log = logging.getLogger("controller.scrubber")


async def scrub_cycle(
    cfg: Settings,
    client: WorkerClient,
    trigger_repair_event: Optional[asyncio.Event] = None,
    scrub_lock: Optional[asyncio.Lock] = None,
) -> dict:
    """Scrub all ALIVE workers and cross-check reported hashes against authoritative DB checksums."""
    lock = scrub_lock or asyncio.Lock()
    async with lock:
        alive_workers = await asyncio.to_thread(db.list_workers, "ALIVE")
        workers_scrubbed = 0
        chunks_checked = 0
        corrupt_found = 0

        for w in alive_workers:
            wid = w["worker_id"]
            try:
                report = await client.scrub_worker(w)
            except WorkerClientError as e:
                log.warning("scrub failed for worker %s: %s", wid, e)
                continue

            workers_scrubbed += 1
            chunks_checked += int(report.get("checked", 0))

            bad_ids = list(report.get("corrupt") or []) + list(report.get("missing_checksum") or [])
            marked_here = set()
            for cid in bad_ids:
                changed = await asyncio.to_thread(db.mark_chunk_replica_corrupt_if_exists, cid, wid)
                if changed or cid not in marked_here:
                    corrupt_found += 1
                    marked_here.add(cid)
                    log.error("scrub detected local corruption chunk=%s worker=%s", cid, wid)

            checksums = report.get("checksums") or {}
            for cid, worker_sha in checksums.items():
                if cid in marked_here:
                    continue
                db_sha = await asyncio.to_thread(db.get_chunk_checksum, cid)
                if db_sha is not None and (worker_sha or "").strip().lower() != db_sha.strip().lower():
                    await asyncio.to_thread(db.mark_chunk_replica_corrupt_if_exists, cid, wid)
                    corrupt_found += 1
                    marked_here.add(cid)
                    log.error(
                        "scrub DB checksum mismatch chunk=%s worker=%s (worker=%s, db=%s)",
                        cid, wid, worker_sha, db_sha,
                    )

        repairs_triggered = False
        if corrupt_found > 0 and trigger_repair_event is not None:
            trigger_repair_event.set()
            repairs_triggered = True

        now = time.time()
        await asyncio.to_thread(db.save_scrub_status, now, chunks_checked, corrupt_found)
        log.info(
            "scrub cycle finished: workers=%d checked=%d corrupt=%d",
            workers_scrubbed, chunks_checked, corrupt_found,
        )
        return {
            "workers_scrubbed": workers_scrubbed,
            "chunks_checked": chunks_checked,
            "corrupt_found": corrupt_found,
            "repairs_triggered": repairs_triggered,
        }


async def scrub_loop(
    cfg: Settings,
    client: WorkerClient,
    trigger_repair_event: asyncio.Event,
    scrub_lock: asyncio.Lock,
) -> None:
    """Periodic background scrub loop."""
    while True:
        await asyncio.sleep(cfg.scrub_interval)
        try:
            await scrub_cycle(
                cfg, client, trigger_repair_event=trigger_repair_event, scrub_lock=scrub_lock
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("scrub cycle failed; will retry")
