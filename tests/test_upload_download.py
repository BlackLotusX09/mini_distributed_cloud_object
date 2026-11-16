import hashlib
import json
import time
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient

import db
import main
import settings as settings_mod

EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class FakeWorkerCluster:
    """In-memory worker cluster driven through httpx.MockTransport."""

    def __init__(self):
        self.stores = {f"worker{i}": {} for i in range(1, 5)}  # {wid: {chunk_id: bytes}}
        self.put_failures = {}    # {wid: "unreachable" | int status | callable}
        self.get_overrides = {}   # {(wid, chunk_id) or wid: bytes | int status | "unreachable"}
        self.delete_failures = set()  # worker_ids where DELETE fails
        self.get_calls = 0

        cluster = self

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            chunk_id = request.url.path.rsplit("/", 1)[-1]
            store = cluster.stores[host]

            if request.method == "PUT":
                fail = cluster.put_failures.get(host)
                if callable(fail):
                    fail = fail(chunk_id, store)
                if fail == "unreachable":
                    raise httpx.ConnectError("worker down", request=request)
                if isinstance(fail, int):
                    return httpx.Response(fail, json={"error": "INJECTED"})
                store[chunk_id] = bytes(request.content)
                return httpx.Response(201, json={"chunk_id": chunk_id})

            if request.method == "GET":
                cluster.get_calls += 1
                ov = cluster.get_overrides.get((host, chunk_id), cluster.get_overrides.get(host))
                if ov == "unreachable":
                    raise httpx.ConnectError("worker down", request=request)
                if isinstance(ov, int):
                    return httpx.Response(ov, json={"error": "INJECTED"})
                if isinstance(ov, bytes):
                    return httpx.Response(200, content=ov)
                if chunk_id not in store:
                    return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND"})
                return httpx.Response(200, content=store[chunk_id])

            if request.method == "DELETE":
                if host in cluster.delete_failures:
                    raise httpx.ConnectError("worker unreachable", request=request)
                store.pop(chunk_id, None)
                return httpx.Response(204)

            return httpx.Response(405)

        self.transport = httpx.MockTransport(handler)


@pytest.fixture
def env(tmp_path):
    cluster = FakeWorkerCluster()
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "controller.db"),
        heartbeat_timeout=15.0,
        liveness_check_interval=60.0,
    )
    app = main.create_app(cfg, worker_transport=cluster.transport)
    with TestClient(app) as client:
        for i in range(1, 5):
            r = client.post(
                "/internal/workers/register",
                json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"},
            )
            assert r.status_code == 200
        yield client, cluster


def assert_error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error", "message"}
    assert body["error"] == code and body["message"]


# --------------------------------------------------------------- upload & round-trip
@pytest.mark.parametrize(
    "size,expected_chunks",
    [
        (0, 0),
        (1, 1),
        (500 * 1024, 1),
        (1_048_576, 1),
        (1_048_577, 2),
        (5 * 1_048_576, 5),
    ],
)
def test_upload_and_download_boundary_sizes(env, size, expected_chunks):
    client, cluster = env
    payload = (b"0123456789abcdef" * ((size // 16) + 1))[:size]
    expected_sha = hashlib.sha256(payload).hexdigest()

    r = client.post(
        "/api/v1/objects",
        files={"file": ("blob.bin", payload, "application/octet-stream")},
        data={"metadata": json.dumps({"tag": "boundary"})},
    )
    assert r.status_code == 201, r.text
    meta = r.json()
    oid = meta["object_id"]
    assert str(uuid.UUID(oid, version=4)) == oid
    assert meta["size"] == size
    assert meta["chunk_count"] == expected_chunks
    assert meta["checksum"] == expected_sha
    assert meta["metadata"] == {"tag": "boundary"}

    # Check chunk IDs and replica distinctness in DB
    obj = db.get_object(oid)
    assert obj["status"] == "COMMITTED"
    assert [c["chunk_id"] for c in obj["chunks"]] == [f"{oid}_{i:06d}" for i in range(expected_chunks)]
    for ch in obj["chunks"]:
        wids = [rep["worker_id"] for rep in ch["replicas"]]
        assert len(wids) == 2 and len(set(wids)) == 2

    # Download and compare byte-for-byte
    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200
    assert dl.content == payload
    assert hashlib.sha256(dl.content).hexdigest() == expected_sha
    assert dl.headers["content-length"] == str(size)
    assert 'filename="blob.bin"' in dl.headers["content-disposition"]


def test_zero_byte_upload_policy(env):
    client, _ = env
    r = client.post("/api/v1/objects", files={"file": ("empty.txt", b"", "text/plain")})
    assert r.status_code == 201
    body = r.json()
    assert body["size"] == 0
    assert body["chunk_count"] == 0
    assert body["checksum"] == EMPTY_SHA256
    dl = client.get(f"/api/v1/objects/{body['object_id']}")
    assert dl.status_code == 200 and dl.content == b""


def test_duplicate_object_names_permitted(env):
    client, _ = env
    r1 = client.post("/api/v1/objects", files={"file": ("same.txt", b"first", "text/plain")})
    r2 = client.post("/api/v1/objects", files={"file": ("same.txt", b"second", "text/plain")})
    assert r1.status_code == 201 and r2.status_code == 201
    oid1, oid2 = r1.json()["object_id"], r2.json()["object_id"]
    assert oid1 != oid2
    assert client.get(f"/api/v1/objects/{oid1}").content == b"first"
    assert client.get(f"/api/v1/objects/{oid2}").content == b"second"


# --------------------------------------------------------------- validation errors
def test_upload_missing_file_returns_400(env):
    client, _ = env
    assert_error(client.post("/api/v1/objects", data={"metadata": "{}"}), 400, "MISSING_FILE")


@pytest.mark.parametrize(
    "raw_meta",
    [
        "{bad json",
        "[]",
        '"just a string"',
        json.dumps({"num": 123}),
        json.dumps({"nil": None}),
        json.dumps({"": "empty-key"}),
        json.dumps({"status": "COMMITTED"}),
        json.dumps({"big": "x" * 17000}),
    ],
)
def test_upload_invalid_metadata_returns_400(env, raw_meta):
    client, _ = env
    r = client.post(
        "/api/v1/objects",
        files={"file": ("a.txt", b"hello", "text/plain")},
        data={"metadata": raw_meta},
    )
    assert_error(r, 400, "INVALID_METADATA")


def test_upload_preflight_insufficient_workers_creates_zero_rows(env):
    client, _ = env
    for i in (1, 2, 3):
        db.mark_dead(f"worker{i}", time.time())
    r = client.post("/api/v1/objects", files={"file": ("a.bin", b"data", "application/octet-stream")})
    assert_error(r, 503, "INSUFFICIENT_WORKERS")
    with db.connection() as c:
        assert c.execute("SELECT COUNT(*) FROM objects").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 0


# --------------------------------------------------------------- failure & rollback
def test_mid_upload_worker_death_reroutes_when_enough_workers_remain(env):
    client, cluster = env
    payload = b"A" * 1_048_576 + b"B" * 100  # 2 chunks
    # worker2 dies after chunk 0 succeeds
    def fail_on_chunk1(chunk_id, _store):
        if chunk_id.endswith("_000001"):
            return "unreachable"
        return None

    cluster.put_failures["worker2"] = fail_on_chunk1
    r = client.post("/api/v1/objects", files={"file": ("two.bin", payload, "application/octet-stream")})
    assert r.status_code == 201, r.text
    oid = r.json()["object_id"]
    assert client.get(f"/api/v1/objects/{oid}").content == payload


def test_mid_upload_total_failure_rolls_back_chunks_and_rows(env):
    client, cluster = env
    payload = b"A" * 1_048_576 + b"B" * 100  # 2 chunks

    # Mark worker3 and worker4 dead; worker1 and worker2 succeed chunk 0, then worker2 goes down on chunk 1
    db.mark_dead("worker3", time.time())
    db.mark_dead("worker4", time.time())

    def fail_chunk1(chunk_id, _store):
        if chunk_id.endswith("_000001"):
            return "unreachable"
        return None

    cluster.put_failures["worker2"] = fail_chunk1
    cluster.delete_failures.add("worker2")  # worker2 is also unreachable during rollback delete

    r = client.post(
        "/api/v1/objects",
        files={"file": ("fail.bin", payload, "application/octet-stream")},
        data={"metadata": json.dumps({"env": "test"})},
    )
    assert_error(r, 503, "STORAGE_UNAVAILABLE")

    # worker1's chunk 0 replica was cleaned up; worker2 was unreachable -> PENDING_DELETE
    assert cluster.stores["worker1"] == {}
    assert len(cluster.stores["worker2"]) == 1  # orphaned chunk 0 on worker2

    with db.connection() as c:
        objs = c.execute("SELECT status FROM objects").fetchall()
        assert [o["status"] for o in objs] == ["DELETING"]
        reps = c.execute("SELECT worker_id, state FROM replicas").fetchall()
        assert [(x["worker_id"], x["state"]) for x in reps] == [("worker2", "PENDING_DELETE")]
        assert c.execute("SELECT COUNT(*) FROM custom_metadata").fetchone()[0] == 0

    assert client.get("/api/v1/objects").json()["total"] == 0


def test_mid_upload_rollback_when_all_workers_reachable_removes_everything(env):
    client, cluster = env
    payload = b"A" * 1_048_576 + b"B" * 100
    db.mark_dead("worker3", time.time())
    db.mark_dead("worker4", time.time())

    # worker2 rejects chunk 1 with 500, but stays reachable for DELETE
    cluster.put_failures["worker2"] = lambda cid, _: 500 if cid.endswith("_000001") else None
    r = client.post("/api/v1/objects", files={"file": ("fail2.bin", payload, "application/octet-stream")})
    assert_error(r, 503, "STORAGE_UNAVAILABLE")

    assert cluster.stores["worker1"] == {}
    assert cluster.stores["worker2"] == {}
    with db.connection() as c:
        for t in ("objects", "chunks", "replicas", "custom_metadata"):
            assert c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] == 0


def test_startup_janitor_cleans_only_stale_pending_objects(tmp_path):
    db_path = str(tmp_path / "janitor.db")
    cluster = FakeWorkerCluster()
    cfg = settings_mod.Settings(db_path=db_path, pending_object_ttl_seconds=600.0)
    db.init_db(db_path)

    now = time.time()
    db.upsert_worker("worker1", "http://worker1:8000", now)
    db.upsert_worker("worker2", "http://worker2:8000", now)

    # Stale PENDING object (20 min old) with a replica on worker1
    db.create_object_with_metadata("stale-obj", "stale.bin", "text/plain", now - 1200, {"a": "1"})
    db.persist_chunk("stale-obj_000000", "stale-obj", 0, 4, "abc", ["worker1", "worker2"])
    cluster.stores["worker1"]["stale-obj_000000"] = b"junk"
    cluster.stores["worker2"]["stale-obj_000000"] = b"junk"

    # Fresh PENDING object (10 s old)
    db.create_object_with_metadata("fresh-obj", "fresh.bin", "text/plain", now - 10, {"b": "2"})
    db.persist_chunk("fresh-obj_000000", "fresh-obj", 0, 4, "def", ["worker1", "worker2"])
    cluster.stores["worker1"]["fresh-obj_000000"] = b"keep"

    app = main.create_app(cfg, worker_transport=cluster.transport)
    with TestClient(app) as client:
        # Wait briefly for background startup janitor to finish
        for _ in range(50):
            if db.get_object_row("stale-obj") is None:
                break
            time.sleep(0.02)

        assert db.get_object_row("stale-obj") is None
        assert "stale-obj_000000" not in cluster.stores["worker1"]
        assert "stale-obj_000000" not in cluster.stores["worker2"]

        assert db.get_object_row("fresh-obj")["status"] == "PENDING"
        assert "fresh-obj_000000" in cluster.stores["worker1"]
        # PENDING objects remain invisible on public endpoints
        assert_error(client.get("/api/v1/objects/fresh-obj"), 404, "OBJECT_NOT_FOUND")


# --------------------------------------------------------------- download & fallback
def test_download_visibility_and_missing(env):
    client, _ = env
    assert_error(client.get("/api/v1/objects/does-not-exist"), 404, "OBJECT_NOT_FOUND")

    now = time.time()
    db.create_object_pending("pend-1", "p.bin", 10, 1, now)
    assert_error(client.get("/api/v1/objects/pend-1"), 404, "OBJECT_NOT_FOUND")

    db.create_object_pending("del-1", "d.bin", 10, 1, now)
    db.commit_object("del-1", now, "sum")
    db.set_object_status("del-1", "DELETING", now)
    assert_error(client.get("/api/v1/objects/del-1"), 404, "OBJECT_NOT_FOUND")


def test_download_preflight_503_when_chunk0_unreachable(env):
    client, cluster = env
    r = client.post("/api/v1/objects", files={"file": ("hi.bin", b"hello world", "application/octet-stream")})
    oid = r.json()["object_id"]
    for wid in ("worker1", "worker2", "worker3", "worker4"):
        cluster.get_overrides[wid] = "unreachable"

    dl = client.get(f"/api/v1/objects/{oid}")
    assert_error(dl, 503, "DATA_UNAVAILABLE")


def test_download_fallback_on_tampered_bytes_marks_corrupt(env):
    client, cluster = env
    payload = b"trusted-payload-bytes" * 100
    r = client.post("/api/v1/objects", files={"file": ("good.bin", payload, "application/octet-stream")})
    oid = r.json()["object_id"]
    obj = db.get_object(oid)
    chunk = obj["chunks"][0]
    w1, w2 = [rep["worker_id"] for rep in chunk["replicas"]]

    # Force w1 to be tried first deterministically by marking w2 DEAD temporarily (ALIVE before DEAD),
    # while still keeping w2 reachable as fallback.
    db.mark_dead(w2, time.time())
    cluster.get_overrides[(w1, chunk["chunk_id"])] = b"X" * len(payload)

    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200
    assert dl.content == payload

    # w1 must be marked CORRUPT in DB; w2 stays OK
    reps = {rep["worker_id"]: rep["state"] for rep in db.get_object(oid)["chunks"][0]["replicas"]}
    assert reps[w1] == "CORRUPT"
    assert reps[w2] == "OK"

    # Revive w2 and ensure subsequent reads succeed and skip CORRUPT w1
    db.mark_alive(w2, time.time())
    calls_before = cluster.get_calls
    dl2 = client.get(f"/api/v1/objects/{oid}")
    assert dl2.status_code == 200 and dl2.content == payload
    assert cluster.get_calls == calls_before + 1  # went straight to w2


def test_download_fallback_when_worker_returns_409_marks_corrupt(env):
    client, cluster = env
    payload = b"hello-409"
    oid = client.post("/api/v1/objects", files={"file": ("x.bin", payload)}).json()["object_id"]
    chunk = db.get_object(oid)["chunks"][0]
    w1, w2 = [rep["worker_id"] for rep in chunk["replicas"]]

    db.mark_dead(w2, time.time())
    cluster.get_overrides[(w1, chunk["chunk_id"])] = 409
    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200 and dl.content == payload
    reps = {rep["worker_id"]: rep["state"] for rep in db.get_object(oid)["chunks"][0]["replicas"]}
    assert reps[w1] == "CORRUPT" and reps[w2] == "OK"


def test_multichunk_download_midstream_fallback_and_sha256_match(env):
    client, cluster = env
    payload = (b"0123456789abcdef" * (1_048_576 // 16)) + b"tail-chunk-bytes"
    expected_sha = hashlib.sha256(payload).hexdigest()
    oid = client.post("/api/v1/objects", files={"file": ("multi.bin", payload)}).json()["object_id"]
    obj = db.get_object(oid)
    chunk1 = obj["chunks"][1]
    w1, w2 = [rep["worker_id"] for rep in chunk1["replicas"]]

    db.mark_dead(w2, time.time())
    cluster.get_overrides[(w1, chunk1["chunk_id"])] = b"bad-chunk1-bytes"

    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200
    assert dl.content == payload
    assert hashlib.sha256(dl.content).hexdigest() == expected_sha
    reps = {rep["worker_id"]: rep["state"] for rep in db.get_object(oid)["chunks"][1]["replicas"]}
    assert reps[w1] == "CORRUPT" and reps[w2] == "OK"


def test_download_500_when_all_replicas_corrupt(env):
    client, cluster = env
    payload = b"corrupt-all"
    oid = client.post("/api/v1/objects", files={"file": ("bad.bin", payload)}).json()["object_id"]
    chunk = db.get_object(oid)["chunks"][0]
    for rep in chunk["replicas"]:
        cluster.get_overrides[(rep["worker_id"], chunk["chunk_id"])] = b"tampered!!!"

    assert_error(client.get(f"/api/v1/objects/{oid}"), 500, "DATA_CORRUPT")
    # Subsequent request sees all replicas already CORRUPT in DB -> still 500 DATA_CORRUPT
    assert_error(client.get(f"/api/v1/objects/{oid}"), 500, "DATA_CORRUPT")


def test_download_503_when_one_corrupt_and_one_unreachable(env):
    client, cluster = env
    payload = b"partial-corrupt"
    oid = client.post("/api/v1/objects", files={"file": ("u.bin", payload)}).json()["object_id"]
    chunk = db.get_object(oid)["chunks"][0]
    w1, w2 = [rep["worker_id"] for rep in chunk["replicas"]]
    db.set_replica_state(chunk["chunk_id"], w1, "CORRUPT")
    cluster.get_overrides[(w2, chunk["chunk_id"])] = "unreachable"
    assert_error(client.get(f"/api/v1/objects/{oid}"), 503, "DATA_UNAVAILABLE")


def test_settings_module_constants():
    assert settings_mod.CHUNK_SIZE == 1_048_576
    assert settings_mod.REPLICATION_FACTOR == 2
    assert settings_mod.UPLOAD_WORKER_TIMEOUT == 10.0
    assert settings_mod.MAX_RETRIES_PER_CHUNK == 3
    assert settings_mod.PENDING_OBJECT_TTL_SECONDS == 600


def test_upload_disconnect_rolls_back(tmp_path):
    import asyncio
    from ingestion import ingest_upload
    from worker_client import WorkerClient

    cluster = FakeWorkerCluster()
    cfg = settings_mod.Settings(db_path=str(tmp_path / "disc.db"))
    db.init_db(cfg.db_path)
    now = time.time()
    db.upsert_worker("worker1", "http://worker1:8000", now)
    db.upsert_worker("worker2", "http://worker2:8000", now)

    class DisconnectingFile:
        def __init__(self):
            self.calls = 0

        async def read(self, n):
            self.calls += 1
            if self.calls == 1:
                return b"A" * 1_048_576
            raise asyncio.CancelledError("client disconnected")

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            with pytest.raises(asyncio.CancelledError):
                await ingest_upload(cfg, wc, DisconnectingFile(), "drop.bin", "application/octet-stream", {"a": "1"})
        finally:
            await wc.close()

    asyncio.run(scenario())
    assert cluster.stores["worker1"] == {}
    assert cluster.stores["worker2"] == {}
    with db.connection() as c:
        for t in ("objects", "chunks", "replicas", "custom_metadata"):
            assert c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] == 0
