"""Day 8 hardening: error contract, id/size bounds, request-id tracing, timeouts, SQL audit."""
import asyncio
import json
import os
import re
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

import db
import main
import obs
import settings as settings_mod
from worker_client import WorkerClient

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")


@pytest.fixture
def client(tmp_path):
    cfg = settings_mod.Settings(db_path=str(tmp_path / "h.db"), liveness_check_interval=60.0)
    seen = []

    def handler(request: httpx.Request):
        seen.append(request)
        if request.method == "PUT":
            return httpx.Response(201)
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(404)

    app = main.create_app(cfg, worker_transport=httpx.MockTransport(handler))
    with TestClient(app, raise_server_exceptions=False) as c:
        c.seen = seen
        for i in range(1, 5):
            c.post("/internal/workers/register",
                   json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"})
        yield c


def is_json_error(r):
    assert r.headers["content-type"].startswith("application/json"), r.text
    b = r.json()
    assert set(b) == {"error", "message"} and isinstance(b["error"], str) and isinstance(b["message"], str)


# ------------------------------------------------------------ error contract
@pytest.mark.parametrize("method,path,status", [
    ("PUT", "/api/v1/objects", 405),
    ("DELETE", "/api/v1/objects", 405),
    ("POST", "/health", 405),
    ("GET", "/no/such/route", 404),
    ("GET", "/api/v1/objects/../../etc/passwd", 404),
    ("GET", "/api/v1/objects/%2e%2e%2fetc%2fpasswd", 404),
    ("GET", "/api/v1/objects/not-a-uuid", 404),
    ("GET", "/api/v1/objects/not-a-uuid/metadata", 404),
    ("DELETE", "/api/v1/objects/not-a-uuid", 404),
    ("GET", "/api/v1/objects?limit=-1", 400),
    ("GET", "/api/v1/objects?limit=abc", 400),
    ("GET", "/api/v1/objects?limit=100000", 400),
    ("GET", "/api/v1/admin/objects/zzz/chunks", 404),
])
def test_every_error_is_json(client, method, path, status):
    r = client.request(method, path)
    assert r.status_code == status, f"Expected {status}, got {r.status_code}: {r.text}"
    is_json_error(r)


def test_unhandled_exception_is_sanitized_500(client, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("secret /internal/path leak")

    monkeypatch.setattr(db, "list_committed", boom)
    r = client.get("/api/v1/objects")
    assert r.status_code == 500
    assert r.json() == {"error": "INTERNAL_ERROR", "message": "Internal server error"}
    assert "secret" not in r.text and "Traceback" not in r.text
    assert r.headers.get("x-request-id")


def test_domain_exceptions_map_in_one_registry(client):
    from errors import (ApiError, DataCorrupt, DataUnavailable, InvalidInput, InvalidMetadata,
                        InvalidPagination, ObjectBusy, ObjectNotFound, StorageUnavailable)
    expected = {ObjectNotFound: 404, ObjectBusy: 409, StorageUnavailable: 503, DataUnavailable: 503,
                DataCorrupt: 500, InvalidMetadata: 400, InvalidPagination: 400, InvalidInput: 400}
    for cls, status in expected.items():
        assert issubclass(cls, ApiError)
        e = cls("x")
        assert e.status == status and e.code


# --------------------------------------------------------------- bounds
def test_object_name_and_metadata_limits(client):
    long_name = "n" * 256 + ".txt"
    r = client.post("/api/v1/objects", files={"file": (long_name, b"x")})
    assert r.status_code == 400 and r.json()["error"] == "INVALID_INPUT", r.text
    ok = client.post("/api/v1/objects", files={"file": ("n" * 255, b"x")})
    assert ok.status_code == 201, ok.text
    too_many = json.dumps({f"k{i}": "v" for i in range(65)})
    r = client.post("/api/v1/objects", files={"file": ("a.txt", b"x")}, data={"metadata": too_many})
    assert r.status_code == 400 and r.json()["error"] == "INVALID_METADATA", r.text
    at_limit = json.dumps({f"k{i}": "v" for i in range(64)})
    assert client.post("/api/v1/objects", files={"file": ("a.txt", b"x")},
                       data={"metadata": at_limit}).status_code == 201


def test_patch_cannot_exceed_key_limit(client):
    oid = client.post("/api/v1/objects", files={"file": ("a", b"x")},
                      data={"metadata": json.dumps({f"k{i}": "v" for i in range(64)})}).json()["object_id"]
    r = client.patch(f"/api/v1/objects/{oid}/metadata", json={"one_more": "v"})
    assert r.status_code == 400 and r.json()["error"] == "INVALID_METADATA", r.text


def test_content_disposition_injection_is_neutralised(client):
    cd = __import__("objects_api").content_disposition('ev"il\r\nX-Injected: 1.txt')
    assert "\r" not in cd and "\n" not in cd
    assert 'filename="ev_il__X-Injected: 1.txt"' in cd


# -------------------------------------------------------- request tracing
def test_request_id_generated_and_echoed(client):
    r = client.get("/health")
    assert uuid.UUID(r.headers["x-request-id"])
    r = client.get("/health", headers={"X-Request-ID": "trace-abc.123"})
    assert r.headers["x-request-id"] == "trace-abc.123"
    r = client.get("/health", headers={"X-Request-ID": "bad id\twith spaces"})
    assert r.headers["x-request-id"] != "bad id\twith spaces"


def test_request_id_forwarded_to_workers(client):
    r = client.post("/api/v1/objects", files={"file": ("a", b"payload")},
                    headers={"X-Request-ID": "trace-upload-1"})
    assert r.status_code == 201, r.text
    puts = [q for q in client.seen if q.method == "PUT"]
    assert puts and all(q.headers["x-request-id"] == "trace-upload-1" for q in puts)


def test_json_log_format_has_required_fields():
    import logging
    rec = logging.LogRecord("controller.x", logging.WARNING, __file__, 1, "read fallback", (), None)
    rec.chunk_id = "c1"
    out = json.loads(obs.JsonFormatter("controller", "controller").format(rec))
    for key in ("timestamp", "level", "component", "node_id", "request_id", "event"):
        assert key in out
    assert out["request_id"] == "background" and out["chunk_id"] == "c1"


# --------------------------------------------------------------- timeouts
def test_every_worker_call_has_explicit_timeout():
    seen = {}

    def handler(request: httpx.Request):
        seen[(request.method, request.url.path)] = request.extensions["timeout"]
        if request.url.path.endswith("/chunks"):
            return httpx.Response(200, json={"chunks": []})
        if request.url.path == "/internal/scrub":
            return httpx.Response(200, json={})
        status = {"GET": 200, "PUT": 201}.get(request.method, 204)
        return httpx.Response(status, content=b"x")

    async def go():
        wc = WorkerClient(timeout=1.5, read_timeout=2.5, delete_timeout=3.5, scrub_timeout=4.5,
                          transport=httpx.MockTransport(handler))
        await wc.start()
        w = {"worker_id": "worker1", "address": "http://worker1:8000"}
        await wc.put_chunk(w, "c", b"x", "h")
        await wc.get_chunk(w, "c")
        await wc.delete_chunk(w, "c")
        await wc.list_worker_chunks(w)
        await wc.scrub_worker(w)
        await wc.close()

    asyncio.run(go())
    read = lambda k: seen[k]["read"]
    assert read(("PUT", "/internal/chunks/c")) == 1.5
    assert read(("GET", "/internal/chunks/c")) == 2.5
    assert read(("DELETE", "/internal/chunks/c")) == 3.5
    assert read(("GET", "/internal/workers/worker1/chunks")) == 2.5
    assert read(("POST", "/internal/scrub")) == 4.5


def test_timeout_settings_from_env():
    s = settings_mod.load({"WORKER_READ_TIMEOUT": "3", "WORKER_DELETE_TIMEOUT": "2", "WORKER_SCRUB_TIMEOUT": "9"})
    assert (s.worker_read_timeout, s.worker_delete_timeout, s.worker_scrub_timeout) == (3.0, 2.0, 9.0)
    d = settings_mod.load({})
    assert (d.worker_read_timeout, d.worker_delete_timeout, d.worker_scrub_timeout) == (10.0, 5.0, 60.0)


# ----------------------------------------------------------- SQL audit
def test_all_sql_is_parameterized():
    ctrl = os.path.join(ROOT, "controller")
    bad = []
    for fn in os.listdir(ctrl):
        if not fn.endswith(".py"):
            continue
        src = open(os.path.join(ctrl, fn)).read()
        for n, line in enumerate(src.splitlines(), 1):
            has_sql = re.search(r"\b(SELECT|INSERT INTO|UPDATE|DELETE FROM)\b", line)
            if has_sql and (re.search(r'\bf["\']', line) or '" +' in line or "%s" in line and "execute" in line
                            or ".format(" in line):
                bad.append(f"{fn}:{n}: {line.strip()}")
    assert not bad, bad


# ------------------------------------------------------------- worker
def test_worker_errors_are_json_and_sanitised(tmp_path):
    import http.client
    import threading
    from app import make_server
    from storage import Storage

    st = Storage(str(tmp_path))
    srv = make_server(st, "w1", host="127.0.0.1", port=0)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]

    def req(method, path, headers=None):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        c.request(method, path, headers=headers or {})
        r = c.getresponse()
        body = r.read()
        c.close()
        return r, body

    try:
        for method in ("PATCH", "OPTIONS", "HEAD"):
            r, body = req(method, "/internal/chunks/x")
            assert r.status == 405, method
            if method != "HEAD":
                assert json.loads(body)["error"] == "METHOD_NOT_ALLOWED"
        r, body = req("GET", "/nope", {"X-Request-ID": "trace-w-1"})
        assert r.status == 404 and r.getheader("X-Request-ID") == "trace-w-1"
        assert json.loads(body)["error"] == "NOT_FOUND"
        st.read_chunk = lambda cid: (_ for _ in ()).throw(RuntimeError("/secret/path"))
        r, body = req("GET", "/internal/chunks/abc")
        assert r.status == 500
        assert json.loads(body) == {"error": "INTERNAL_ERROR", "message": "Internal server error"}
    finally:
        srv.shutdown()
        srv.server_close()


def test_worker_path_stays_inside_root(tmp_path):
    from storage import InvalidChunkIDException, Storage
    st = Storage(str(tmp_path))
    for bad in ("../escape", "a/../../b", "..", "a\x00b"):
        with pytest.raises(InvalidChunkIDException):
            st.write_chunk(bad, b"x")


def test_check_invariants_detects_all_violations(tmp_path):
    import sys
    import time
    from storage import Storage, sha256_hex

    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    import check_invariants as ci

    db_path = str(tmp_path / "inv.db")
    db.init_db(db_path)
    now = 10_000.0
    for wid in ("worker1", "worker2", "worker3"):
        db.upsert_worker(wid, f"http://{wid}:8000", now)
        Storage(str(tmp_path / wid))

    oid = str(uuid.uuid4())
    cid = f"{oid}_000000"
    data = b"invariant-bytes"
    digest = sha256_hex(data)
    db.create_object_pending(oid, "ok.bin", len(data), 1, now, checksum=digest)
    db.persist_chunk(cid, oid, 0, len(data), digest, ["worker1", "worker2"])
    db.commit_object(oid, now, digest)
    Storage(str(tmp_path / "worker1")).write_chunk(cid, data)
    Storage(str(tmp_path / "worker2")).write_chunk(cid, data)

    src = ci.LocalSource(db_path, str(tmp_path))
    assert ci.check(src, ttl=600.0, now=now + 10) == []

    # Violation 1: under-replicated COMMITTED chunk
    db.set_replica_state(cid, "worker2", "CORRUPT")
    v1 = ci.check(src, ttl=600.0, now=now + 10)
    assert any(v.startswith("[1]") for v in v1)
    db.set_replica_state(cid, "worker2", "OK")

    # Violation 2: OK row whose on-disk chunk SHA differs
    p2 = os.path.join(str(tmp_path / "worker2"), "chunks", cid[:2], f"{cid}.chunk")
    with open(p2, "wb") as f:
        f.write(b"corrupted-bytes")
    v2 = ci.check(src, ttl=600.0, now=now + 10)
    assert any(v.startswith("[2]") for v in v2)
    Storage(str(tmp_path / "worker2")).write_chunk(cid, data)

    # Violation 3: orphan chunk file on disk older than TTL without DB row
    orphan_id = f"{uuid.uuid4()}_000000"
    Storage(str(tmp_path / "worker3")).write_chunk(orphan_id, b"orphan")
    assert ci.check(src, ttl=600.0, now=time.time() + 10) == []  # within TTL -> exempt
    v3 = ci.check(src, ttl=600.0, now=time.time() + 700.0)
    assert any(v.startswith("[3]") for v in v3)
    Storage(str(tmp_path / "worker3")).delete_chunk(orphan_id)

    # Violation 4: stale PENDING object older than TTL
    pend_id = str(uuid.uuid4())
    db.create_object_pending(pend_id, "stale.bin", 0, 0, now - 700.0)
    v4 = ci.check(src, ttl=600.0, now=now)
    assert any(v.startswith("[4]") for v in v4)

