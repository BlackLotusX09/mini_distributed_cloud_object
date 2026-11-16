"""Public object API: upload, download, metadata, list, delete."""
import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from typing import List
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException

import db
import validation
from errors import ApiError
from ingestion import ingest_upload
from lifecycle import delete_replicas
from worker_client import WorkerClient, WorkerClientError, WorkerRejected

log = logging.getLogger("controller.objects")
router = APIRouter()

SYSTEM_FIELDS = ("object_id", "name", "size", "content_type", "checksum",
                 "chunk_count", "created_at", "modified_at")


def _system_view(row: dict) -> dict:
    return {k: row[k] for k in SYSTEM_FIELDS}


def not_found(object_id: str) -> ApiError:
    return ApiError(404, "OBJECT_NOT_FOUND", f"object '{object_id}' not found")


async def _committed_row(object_id: str) -> dict:
    row = await asyncio.to_thread(db.get_object_row, object_id)
    if row is None or row["status"] != "COMMITTED":  # PENDING / DELETING look like "missing"
        raise not_found(object_id)
    return row


# ------------------------------------------------------------------ upload
@router.post("/api/v1/objects", status_code=201)
async def upload_object(request: Request):
    cfg, client = request.app.state.cfg, request.app.state.worker_client
    try:
        form = await request.form()  # file parts are spooled to disk beyond 1 MiB
    except MultiPartException as e:
        raise ApiError(400, "INVALID_REQUEST", f"malformed multipart body: {e.message}") from None
    try:
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise ApiError(400, "MISSING_FILE", "multipart field 'file' is required")
        custom = validation.parse_upload_metadata(form.get("metadata"), cfg.max_metadata_bytes)
        name = os.path.basename((file.filename or "").replace("\\", "/")).strip()[:255] or "unnamed"
        content_type = file.content_type or "application/octet-stream"
        row = await ingest_upload(cfg, client, file, name, content_type, custom)
        return JSONResponse(status_code=201, content={**_system_view(row), "metadata": custom})
    finally:
        await form.close()


# ---------------------------------------------------------------- download
class _Unavailable(ApiError):
    def __init__(self, chunk_id):
        super().__init__(503, "DATA_UNAVAILABLE", f"no reachable replica for chunk {chunk_id}")


class _Corrupt(ApiError):
    def __init__(self, chunk_id):
        super().__init__(500, "DATA_CORRUPT", f"all available replicas of chunk {chunk_id} are corrupt")


async def fetch_verified_chunk(client: WorkerClient, chunk: dict, workers: dict, rng=random) -> bytes:
    """Fetch one chunk, verifying SHA-256 against the DB; fall back replica by replica."""
    chunk_id = chunk["chunk_id"]
    usable = [r for r in chunk["replicas"] if r["state"] == "OK" and r["worker_id"] in workers]
    if not usable:
        if any(r["state"] == "CORRUPT" for r in chunk["replicas"]):
            raise _Corrupt(chunk_id)
        raise _Unavailable(chunk_id)

    alive = [r for r in usable if workers[r["worker_id"]]["status"] == "ALIVE"]
    dead = [r for r in usable if workers[r["worker_id"]]["status"] != "ALIVE"]
    rng.shuffle(alive)  # spread read load
    rng.shuffle(dead)

    corrupt_this_read = False
    for r in alive + dead:  # DEAD workers only as a last resort
        worker = workers[r["worker_id"]]
        try:
            data = await client.get_chunk(worker, chunk_id)
        except WorkerRejected as e:  # 404 missing / 409 corrupt: this replica is no good
            log.warning("chunk=%s worker=%s rejected (%s); marking CORRUPT", chunk_id, r["worker_id"], e)
            await asyncio.to_thread(db.set_replica_state, chunk_id, r["worker_id"], "CORRUPT")
            if e.status == 409:
                corrupt_this_read = True
            continue
        except WorkerClientError as e:  # unreachable / 5xx: transient, don't condemn the replica
            log.warning("chunk=%s worker=%s unavailable (%s)", chunk_id, r["worker_id"], e)
            continue
        if len(data) != chunk["size"] or hashlib.sha256(data).hexdigest() != chunk["checksum"]:
            log.error("chunk=%s worker=%s returned data failing checksum; marking CORRUPT",
                      chunk_id, r["worker_id"])
            await asyncio.to_thread(db.set_replica_state, chunk_id, r["worker_id"], "CORRUPT")
            corrupt_this_read = True
            continue
        return data

    raise _Corrupt(chunk_id) if corrupt_this_read else _Unavailable(chunk_id)


async def stream_chunks(client, first: bytes, rest: List[dict], workers: dict, expected_checksum: str,
                        object_id: str = "", rng=random):
    """Yield chunk 0 (already verified), then fetch the others one at a time."""
    whole = hashlib.sha256()
    try:
        whole.update(first)
        yield first
        for chunk in rest:
            data = await fetch_verified_chunk(client, chunk, workers, rng)
            whole.update(data)
            yield data
            del data
    except ApiError as e:
        log.error("download of %s aborted mid-stream: %s", object_id, e.message)
        raise
    if whole.hexdigest() != expected_checksum:
        log.critical("INTEGRITY ERROR: object %s whole-object checksum mismatch "
                     "(expected %s, got %s)", object_id, expected_checksum, whole.hexdigest())


def content_disposition(filename: str) -> str:
    ascii_name = re.sub(r"[^\x20-\x7e]", "_", filename).replace("\\", "_").replace('"', "_")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{quote(filename, safe='')}"


@router.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str, request: Request):
    client = request.app.state.worker_client
    obj = await asyncio.to_thread(db.get_object, object_id)
    if obj is None or obj["status"] != "COMMITTED":
        raise not_found(object_id)
    headers = {
        "Content-Length": str(obj["size"]),
        "Content-Disposition": content_disposition(obj["name"]),
    }
    media_type = obj["content_type"] or "application/octet-stream"
    chunks = obj["chunks"]
    if obj["chunk_count"] == 0 or not chunks:
        return Response(content=b"", media_type=media_type, headers=headers)

    workers = {w["worker_id"]: w for w in await asyncio.to_thread(db.list_workers)}
    # Pre-flight: fail with a proper 503/500 before any header is sent.
    first = await fetch_verified_chunk(client, chunks[0], workers)
    return StreamingResponse(
        stream_chunks(client, first, chunks[1:], workers, obj["checksum"], object_id),
        media_type=media_type, headers=headers,
    )


# ---------------------------------------------------------------- metadata
def _metadata_view(row: dict, custom: dict) -> dict:
    return {**_system_view(row), "custom": custom}


@router.get("/api/v1/objects/{object_id}/metadata")
async def get_metadata(object_id: str):  # never contacts a worker
    row = await _committed_row(object_id)
    custom = await asyncio.to_thread(db.get_custom_metadata, object_id)
    return _metadata_view(row, custom)


@router.patch("/api/v1/objects/{object_id}/metadata")
async def patch_metadata(object_id: str, request: Request):
    cfg = request.app.state.cfg
    try:
        body = await request.json()
    except ValueError:
        raise ApiError(400, "INVALID_METADATA", "request body must be valid JSON") from None
    patch = validation.validate_patch(body, cfg.max_metadata_bytes)
    try:
        custom = await asyncio.to_thread(db.patch_metadata, object_id, patch, time.time(),
                                         cfg.max_metadata_bytes)
    except db.ObjectNotFoundError:
        raise not_found(object_id) from None
    except db.MetadataTooLargeError:
        raise ApiError(400, "INVALID_METADATA",
                       f"merged metadata exceeds {cfg.max_metadata_bytes} bytes") from None
    row = await asyncio.to_thread(db.get_object_row, object_id)
    return _metadata_view(row, custom)


# -------------------------------------------------------------------- list
@router.get("/api/v1/objects")
async def list_objects(request: Request):
    limit, offset = validation.parse_pagination(request.query_params)
    rows, total = await asyncio.to_thread(db.list_committed, limit, offset)
    return {"objects": rows, "total": total, "limit": limit, "offset": offset}


# ------------------------------------------------------------------ delete
@router.delete("/api/v1/objects/{object_id}", status_code=204)
async def delete_object(object_id: str, request: Request):
    client = request.app.state.worker_client
    outcome, replicas = await asyncio.to_thread(db.begin_delete, object_id, time.time())
    if outcome == "not_found":
        raise not_found(object_id)
    if outcome == "busy":
        raise ApiError(409, "OBJECT_BUSY", f"object '{object_id}' is still being uploaded")
    deleted, failed = await delete_replicas(client, replicas)
    await asyncio.to_thread(db.apply_replica_cleanup, deleted, failed)
    removed = await asyncio.to_thread(db.finish_delete_if_empty, object_id)
    if not removed:
        log.warning("object %s: %d replica(s) left PENDING_DELETE for the reconciler",
                    object_id, len(failed))
    return Response(status_code=204)

