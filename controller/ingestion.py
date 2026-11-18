"""Streaming upload pipeline (Day 4): one chunk in memory at a time, rollback on any failure."""
import asyncio
import hashlib
import logging
import time
import uuid
from typing import Dict, Optional, Tuple

import db
from errors import StorageUnavailable
from lifecycle import purge_object
from placement import InsufficientWorkersError
from replication import ChunkStoreFailed, store_chunk
from settings import Settings
from worker_client import WorkerClient

log = logging.getLogger("controller.upload")


async def read_chunk(file, size: int) -> bytes:
    """Read up to `size` bytes, looping over short reads; b'' only at EOF."""
    parts, got = [], 0
    while got < size:
        block = await file.read(size - got)
        if not block:
            break
        parts.append(block)
        got += len(block)
    return b"".join(parts)


async def ingest_upload(
    cfg: Settings,
    client: WorkerClient,
    file,
    name: str,
    content_type: str,
    custom: Dict[str, str],
) -> dict:
    """Run steps A-D. `file` needs `async read(n)`. Returns the object row (COMMITTED)."""
    rf = cfg.replication_factor
    alive = await asyncio.to_thread(db.list_alive_with_counts)
    if len(alive) < rf:  # pre-flight: before any DB row or file read
        raise InsufficientWorkersError(needed=rf, available=len(alive))

    object_id = str(uuid.uuid4())
    # Step A: PENDING object (+ metadata) in one transaction
    await asyncio.to_thread(db.create_object_with_metadata, object_id, name, content_type,
                            time.time(), custom)

    committed = False
    inflight: Optional[Tuple[str, Dict[str, dict]]] = None
    try:
        # Step B: stream chunk by chunk
        whole = hashlib.sha256()
        total_size = 0
        idx = 0
        while True:
            data = await read_chunk(file, cfg.chunk_size)
            if not data:
                break
            checksum = hashlib.sha256(data).hexdigest()
            whole.update(data)
            total_size += len(data)
            chunk_id = f"{object_id}_{idx:06d}"

            alive = await asyncio.to_thread(db.list_alive_with_counts)  # fresh view per chunk
            touched: Dict[str, dict] = {}
            inflight = (chunk_id, touched)
            workers = await store_chunk(
                chunk_id, data, checksum, alive, client,
                replication_factor=rf, max_retries=cfg.max_retries_per_chunk, touched=touched,
            )
            await asyncio.to_thread(db.persist_chunk, chunk_id, object_id, idx, len(data),
                                    checksum, workers)
            inflight = None  # now fully recorded in the DB
            idx += 1
            del data

        # Step C: commit
        ok = await asyncio.to_thread(db.finalize_object, object_id, total_size,
                                     whole.hexdigest(), idx, time.time())
        if not ok:
            raise RuntimeError(f"object {object_id} was no longer PENDING at commit time")
        committed = True
    except BaseException as exc:  # includes CancelledError / client disconnect
        if not committed:
            log.warning("upload of %s failed (%s: %s); rolling back", object_id, type(exc).__name__, exc)
            try:
                await asyncio.shield(purge_object(client, object_id, inflight))
            except Exception:
                log.exception("rollback of %s failed", object_id)
        if isinstance(exc, ChunkStoreFailed):
            raise StorageUnavailable(str(exc)) from exc
        raise

    return await asyncio.to_thread(db.get_object_row, object_id)

