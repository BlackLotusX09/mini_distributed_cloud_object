"""Replicate one chunk onto R distinct workers, tolerating partial failure."""
import asyncio
import logging
import random
from typing import Dict, List, Optional

import worker_client
from placement import choose_workers
from settings import MAX_RETRIES_PER_CHUNK, REPLICATION_FACTOR
from worker_client import WorkerClient

log = logging.getLogger("controller.replication")


class ChunkStoreFailed(Exception):
    def __init__(self, message: str, acked: Optional[List[str]] = None):
        super().__init__(message)
        self.acked = list(acked or [])  # workers that DO hold the chunk (need cleanup)


def _normalize_alive_workers(alive_workers: List[dict]) -> Dict[str, dict]:
    by_id: Dict[str, dict] = {}
    for w in alive_workers:
        if isinstance(w, dict):
            wid = w["worker_id"]
            status = w.get("status", "ALIVE")
            if status != "ALIVE":
                continue
            by_id[wid] = {
                "worker_id": wid,
                "address": w.get("address") or f"http://{wid}:8000",
                "status": status,
                "chunk_count": int(w.get("chunk_count", 0)),
            }
        else:
            wid = getattr(w, "worker_id")
            status = getattr(w, "status", "ALIVE")
            if status != "ALIVE":
                continue
            by_id[wid] = {
                "worker_id": wid,
                "address": getattr(w, "address", None) or f"http://{wid}:8000",
                "status": status,
                "chunk_count": int(getattr(w, "chunk_count", 0)),
            }
    return by_id


async def store_chunk(
    chunk_id: str,
    data: bytes,
    checksum: str,
    alive_workers: List[dict],
    client: Optional[WorkerClient] = None,
    replication_factor: int = REPLICATION_FACTOR,
    max_retries: int = MAX_RETRIES_PER_CHUNK,
    touched: Optional[Dict[str, dict]] = None,
    rng=random,
) -> List[str]:
    """Return the `replication_factor` distinct worker_ids holding a verified replica.

    - acknowledged replicas are never cancelled or discarded;
    - only the missing replicas are retried, at most `max_retries` rounds;
    - a retry never targets a worker that already acked or already failed this chunk;
    - `touched` (optional out-param) collects every worker a PUT was sent to, so the
      caller can clean up an in-flight chunk if the whole upload is rolled back.
    """
    n = replication_factor
    by_id = _normalize_alive_workers(alive_workers)
    if len(by_id) < n:
        raise ChunkStoreFailed(f"need {n} alive workers, have {len(by_id)}")

    send_put = client.put_chunk if client is not None else worker_client.put_chunk

    acked: List[str] = []
    failed: set = set()
    targets = choose_workers(list(by_id.values()), n, rng=rng)
    attempt = 0
    while True:
        if touched is not None:
            for wid in targets:
                touched[wid] = by_id[wid]
        results = await asyncio.gather(
            *(send_put(by_id[wid], chunk_id, data, checksum) for wid in targets),
            return_exceptions=True,
        )
        for wid, result in zip(targets, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                failed.add(wid)
                log.warning("chunk=%s worker=%s attempt=%d outcome=FAILED (%s)",
                            chunk_id, wid, attempt, result)
            else:
                acked.append(wid)
                log.info("chunk=%s worker=%s attempt=%d outcome=ACK", chunk_id, wid, attempt)

        missing = n - len(acked)
        if missing <= 0:
            return acked[:n]
        attempt += 1
        if attempt > max_retries:
            raise ChunkStoreFailed(
                f"chunk {chunk_id}: only {len(acked)}/{n} replicas after {max_retries} retries", acked)
        candidates = [w for wid, w in by_id.items() if wid not in acked and wid not in failed]
        if len(candidates) < missing:
            raise ChunkStoreFailed(
                f"chunk {chunk_id}: {len(acked)}/{n} replicas and only {len(candidates)} "
                f"untried workers left", acked)
        targets = choose_workers(candidates, missing, rng=rng)
