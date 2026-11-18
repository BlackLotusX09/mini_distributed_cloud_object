"""Endpoint contract suite: every endpoint x status code against a live cluster."""
import io
import json
import time
import uuid
import zipfile

import pytest

from helpers import (expect, expect_error, get_chunk_map, random_bytes, sha256_hex, wait_for,
                     wait_worker)

pytestmark = pytest.mark.integration

EMPTY_SHA = sha256_hex(b"")


def upload_raw(client, data, name="f.bin", ctype="application/octet-stream", metadata=None, **kw):
    form = {"metadata": metadata if isinstance(metadata, str) else json.dumps(metadata)} if metadata is not None else {}
    return client.post("/api/v1/objects", files={"file": (name, data, ctype)}, data=form, **kw)


# ----------------------------------------------------------------- upload
@pytest.mark.parametrize("size,chunks", [(0, 0), (1, 1), (500 * 1024, 1), (1_048_576, 1),
                                         (1_048_577, 2), (5 * 1_048_576, 5)])
def test_upload_boundary_sizes(client, uploaded_object, size, chunks):
    oid, raw, digest = uploaded_object(size, metadata={"k": "v"})
    meta = expect(client.get(f"/api/v1/objects/{oid}/metadata"), 200).json()
    assert meta["size"] == size and meta["chunk_count"] == chunks
    assert meta["checksum"] == digest
    assert meta["custom"] == {"k": "v"}
    assert str(uuid.UUID(oid, version=4)) == oid
    dl = expect(client.get(f"/api/v1/objects/{oid}"), 200)
    assert sha256_hex(dl.content) == digest and len(dl.content) == size


def test_duplicate_filenames_get_unique_ids(client, uploaded_object):
    a, _, _ = uploaded_object(10, name="same.txt")
    b, _, _ = uploaded_object(10, name="same.txt")
    assert a != b
    names = {client.get(f"/api/v1/objects/{o}/metadata").json()["name"] for o in (a, b)}
    assert names == {"same.txt"}


def test_upload_rejections(client):
    expect_error(client.post("/api/v1/objects", data={"metadata": "{}"}), 400, "MISSING_FILE")
    expect_error(upload_raw(client, b"x", metadata="{bad json"), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", metadata="[1,2]"), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", metadata=json.dumps({"big": "x" * 17000})), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", metadata=json.dumps({"n": 1})), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", metadata=json.dumps({"status": "COMMITTED"})), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", metadata=json.dumps({f"k{i}": "v" for i in range(65)})), 400, "INVALID_METADATA")
    expect_error(upload_raw(client, b"x", name="n" * 300), 400, "INVALID_INPUT")


# --------------------------------------------------------------- download
def _png():
    return b"\x89PNG\r\n\x1a\n" + random_bytes(2048)


def _zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.txt", "hello " * 500)
    return buf.getvalue()


@pytest.mark.parametrize("name,ctype,make", [
    ("note.txt", "text/plain", lambda: ("unicode ✓ text\n" * 5000).encode()),
    ("blob.bin", "application/octet-stream", lambda: random_bytes(300_000)),
    ("pic.png", "image/png", _png),
    ("arch.zip", "application/zip", _zip),
])
def test_download_exact_bytes_and_headers(client, uploaded_object, name, ctype, make):
    raw = make()
    oid, _, digest = uploaded_object(name=name, content_type=ctype, data=raw)
    r = expect(client.get(f"/api/v1/objects/{oid}"), 200)
    assert sha256_hex(r.content) == digest
    assert r.headers["content-type"].startswith(ctype)
    assert r.headers["content-length"] == str(len(raw))
    assert f'filename="{name}"' in r.headers["content-disposition"]


def test_download_unknown_and_malformed_ids(client):
    expect_error(client.get(f"/api/v1/objects/{uuid.uuid4()}"), 404, "OBJECT_NOT_FOUND")
    for bad in ("..%2Fetc%2Fpasswd", "not-a-uuid", "%2e%2e", "a" * 300):
        r = client.get(f"/api/v1/objects/{bad}")
        assert r.status_code in (400, 404), f"Expected 400/404, got {r.status_code}: {r.text}"
        assert r.headers["content-type"].startswith("application/json")
        assert "root:" not in r.text


# --------------------------------------------------------------- metadata
def test_metadata_get_works_with_all_workers_stopped(cluster, client, uploaded_object):
    oid, _, _ = uploaded_object(2048, metadata={"owner": "alice"})
    from backends import WORKERS
    for w in WORKERS:
        cluster.stop_worker(w)
    try:
        r = expect(client.get(f"/api/v1/objects/{oid}/metadata"), 200)  # zero worker contact
        assert r.json()["custom"] == {"owner": "alice"}
        expect(client.get("/api/v1/objects?limit=5"), 200)
    finally:
        cluster.restart_workers(WORKERS)
        for w in WORKERS:
            wait_worker(client, w, "ALIVE", timeout=90)


def test_patch_metadata_semantics(client, uploaded_object):
    oid, _, digest = uploaded_object(3 * 1024 * 1024, metadata={"a": "1", "b": "2"})
    before = client.get(f"/api/v1/objects/{oid}/metadata").json()
    cmap_before = get_chunk_map(client, oid)
    time.sleep(0.05)
    r = expect(client.patch(f"/api/v1/objects/{oid}/metadata", json={"a": "one", "c": "3", "b": None}), 200)
    body = r.json()
    assert body["custom"] == {"a": "one", "c": "3"}
    assert body["modified_at"] > before["modified_at"] and body["created_at"] == before["created_at"]
    assert body["checksum"] == digest and body["size"] == before["size"]
    assert [(c["chunk_id"], c["checksum"]) for c in get_chunk_map(client, oid)] == \
           [(c["chunk_id"], c["checksum"]) for c in cmap_before]
    for bad in ({"status": "x"}, {"object_id": "x"}, {"k": 5}, {"k": ["a"]}, {"": "v"}):
        expect_error(client.patch(f"/api/v1/objects/{oid}/metadata", json=bad), 400, "INVALID_METADATA")
    expect_error(client.patch(f"/api/v1/objects/{oid}/metadata", content=b"nope",
                              headers={"content-type": "application/json"}), 400, "INVALID_METADATA")
    expect_error(client.patch(f"/api/v1/objects/{uuid.uuid4()}/metadata", json={"a": "b"}), 404, "OBJECT_NOT_FOUND")


# ------------------------------------------------------------------- list
def _walk(client, limit):
    ids, offset, total = [], 0, None
    while True:
        r = expect(client.get(f"/api/v1/objects?limit={limit}&offset={offset}"), 200).json()
        total = r["total"]
        ids += [o["object_id"] for o in r["objects"]]
        offset += limit
        if offset >= total:
            return ids, total


def test_list_pagination_exact(client, uploaded_object):
    before = client.get("/api/v1/objects?limit=1").json()["total"]
    mine = {uploaded_object(100 + i, metadata={"t": "list"})[0] for i in range(7)}
    ids, total = _walk(client, 3)
    assert total == before + 7 and len(ids) == total, "pages must cover total exactly"
    assert len(set(ids)) == len(ids), "duplicates across pages"
    assert mine <= set(ids)
    for bad in ("limit=-1", "limit=abc", "limit=100000", "offset=-1", "offset=x", "limit=0"):
        expect_error(client.get(f"/api/v1/objects?{bad}"), 400, "INVALID_PAGINATION")


def test_list_excludes_pending_and_deleting(cluster, client):
    ids = {s: str(uuid.uuid4()) for s in ("PENDING", "DELETING")}
    now = time.time()
    try:
        for status, oid in ids.items():
            cluster.db_write("INSERT INTO objects (object_id,name,size,content_type,checksum,chunk_count,"
                             "status,created_at,modified_at) VALUES (?,?,?,?,?,?,?,?,?)",
                             (oid, f"{status}.bin", 0, None, None, 0, status, now, now))
        listed, _ = _walk(client, 50)
        assert not set(ids.values()) & set(listed)
        for oid in ids.values():
            expect_error(client.get(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
            expect_error(client.get(f"/api/v1/objects/{oid}/metadata"), 404, "OBJECT_NOT_FOUND")
            expect_error(client.patch(f"/api/v1/objects/{oid}/metadata", json={"a": "b"}), 404, "OBJECT_NOT_FOUND")
        expect_error(client.delete(f"/api/v1/objects/{ids['PENDING']}"), 409, "OBJECT_BUSY")
        expect_error(client.delete(f"/api/v1/objects/{ids['DELETING']}"), 404, "OBJECT_NOT_FOUND")
    finally:
        for oid in ids.values():
            cluster.db_write("DELETE FROM objects WHERE object_id = ?", (oid,))


# ----------------------------------------------------------------- delete
def test_delete_lifecycle_and_chunk_cleanup(cluster, client, uploaded_object):
    from backends import WORKERS
    oid, _, _ = uploaded_object(3 * 1024 * 1024)
    cids = {c["chunk_id"] for c in get_chunk_map(client, oid)}
    assert any(cids & set(cluster.worker_files(w)) for w in WORKERS)
    expect(client.delete(f"/api/v1/objects/{oid}"), 204)
    expect_error(client.get(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    expect_error(client.get(f"/api/v1/objects/{oid}/metadata"), 404, "OBJECT_NOT_FOUND")
    expect_error(client.delete(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    assert oid not in _walk(client, 50)[0]
    wait_for(lambda: not any(cids & set(cluster.worker_files(w)) for w in WORKERS),
             timeout=30, desc="chunk files removed from all worker disks")


# ------------------------------------------------------------ error shape
@pytest.mark.parametrize("method,path,status", [
    ("PUT", "/api/v1/objects", 405),
    ("PATCH", "/api/v1/objects", 405),
    ("DELETE", "/api/v1/objects", 405),
    ("POST", "/api/v1/objects/" + str(uuid.uuid4()), 405),
    ("GET", "/no/such/path", 404),
    ("GET", "/api/v1/objects/" + str(uuid.uuid4()), 404),
    ("DELETE", "/api/v1/objects/" + str(uuid.uuid4()), 404),
    ("GET", "/api/v1/objects/zzz", 404),
    ("GET", "/api/v1/objects?limit=-1", 400),
    ("GET", "/api/v1/admin/objects/" + str(uuid.uuid4()) + "/chunks", 404),
    ("POST", "/internal/workers/register", 400),
    ("POST", "/internal/workers/heartbeat", 400),
])
def test_all_errors_are_json_with_error_and_message(client, method, path, status):
    r = client.request(method, path)
    assert r.status_code == status, f"Expected {status}, got {r.status_code}: {r.text}"
    assert r.headers["content-type"].startswith("application/json")
    body = r.json()
    assert isinstance(body.get("error"), str) and isinstance(body.get("message"), str), body
    assert "detail" not in body and "Traceback" not in r.text
    assert r.headers.get("x-request-id")


def test_request_id_round_trip(client):
    r = client.get("/health", headers={"X-Request-ID": "it-trace-42"})
    assert r.headers["x-request-id"] == "it-trace-42"
