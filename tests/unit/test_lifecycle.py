import concurrent.futures
import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import db
import main
import settings as settings_mod


class SimpleCluster:
    def __init__(self):
        self.stores = {f"worker{i}": {} for i in range(1, 5)}
        self.dead_for_delete = set()
        self.worker_requests = 0

        cluster = self

        def handler(request: httpx.Request) -> httpx.Response:
            cluster.worker_requests += 1
            host = request.url.host
            cid = request.url.path.rsplit("/", 1)[-1]
            if request.method == "PUT":
                cluster.stores[host][cid] = bytes(request.content)
                return httpx.Response(201)
            if request.method == "GET":
                if cid not in cluster.stores[host]:
                    return httpx.Response(404)
                return httpx.Response(200, content=cluster.stores[host][cid])
            if request.method == "DELETE":
                if host in cluster.dead_for_delete:
                    raise httpx.ConnectError("dead", request=request)
                cluster.stores[host].pop(cid, None)
                return httpx.Response(204)
            return httpx.Response(405)

        self.transport = httpx.MockTransport(handler)


@pytest.fixture
def env(tmp_path):
    cluster = SimpleCluster()
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "controller.db"),
        liveness_check_interval=60.0,
    )
    app = main.create_app(cfg, worker_transport=cluster.transport)
    with TestClient(app) as client:
        for i in range(1, 5):
            client.post(
                "/internal/workers/register",
                json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"},
            )
        yield client, cluster


def upload(client, name="doc.txt", data=b"hello", meta=None):
    form = {"metadata": json.dumps(meta)} if meta is not None else {}
    return client.post(
        "/api/v1/objects",
        files={"file": (name, data, "text/plain")},
        data=form,
    ).json()


def assert_error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error", "message"}
    assert body["error"] == code and body["message"]


# --------------------------------------------------------------- metadata GET & PATCH
def test_get_metadata_never_contacts_workers_and_hides_non_committed(env):
    client, cluster = env
    obj = upload(client, "notes.txt", b"abc", {"owner": "alice", "env": "dev"})
    oid = obj["object_id"]

    before = cluster.worker_requests
    r = client.get(f"/api/v1/objects/{oid}/metadata")
    assert cluster.worker_requests == before  # zero worker network calls
    assert r.status_code == 200
    body = r.json()
    assert body["object_id"] == oid
    assert body["name"] == "notes.txt"
    assert body["size"] == 3
    assert body["custom"] == {"env": "dev", "owner": "alice"}

    now = time.time()
    db.create_object_pending("pend", "p", 1, 1, now)
    assert_error(client.get("/api/v1/objects/pend/metadata"), 404, "OBJECT_NOT_FOUND")
    db.set_object_status("pend", "DELETING", now)
    assert_error(client.get("/api/v1/objects/pend/metadata"), 404, "OBJECT_NOT_FOUND")
    assert_error(client.get("/api/v1/objects/missing/metadata"), 404, "OBJECT_NOT_FOUND")


def test_patch_metadata_merge_delete_and_validation(env):
    client, _ = env
    oid = upload(client, "m.txt", b"x", {"a": "1", "b": "2"})["object_id"]

    r = client.patch(f"/api/v1/objects/{oid}/metadata", json={"b": None, "c": "3", "a": "updated"})
    assert r.status_code == 200
    assert r.json()["custom"] == {"a": "updated", "c": "3"}

    # Reserved keys blocked
    for reserved in ("id", "object_id", "name", "size", "content_type", "checksum",
                     "chunk_count", "status", "created_at", "modified_at"):
        assert_error(
            client.patch(f"/api/v1/objects/{oid}/metadata", json={reserved: "x"}),
            400, "INVALID_METADATA",
        )

    # Non-string values, non-object bodies, oversized payload
    assert_error(client.patch(f"/api/v1/objects/{oid}/metadata", json={"k": 42}), 400, "INVALID_METADATA")
    assert_error(client.patch(f"/api/v1/objects/{oid}/metadata", json=["not-dict"]), 400, "INVALID_METADATA")
    assert_error(client.patch(f"/api/v1/objects/{oid}/metadata", json={"big": "z" * 17000}), 400, "INVALID_METADATA")

    # Visibility guard
    db.create_object_pending("pend2", "p", 1, 1, time.time())
    assert_error(client.patch("/api/v1/objects/pend2/metadata", json={"k": "v"}), 404, "OBJECT_NOT_FOUND")
    assert_error(client.patch("/api/v1/objects/missing/metadata", json={"k": "v"}), 404, "OBJECT_NOT_FOUND")


def test_concurrent_patch_metadata_no_lost_updates(env):
    client, _ = env
    oid = upload(client, "conc.txt", b"x", {"init": "0"})["object_id"]

    def worker_patch(i):
        return client.patch(f"/api/v1/objects/{oid}/metadata", json={f"k{i}": f"v{i}"}).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        statuses = list(pool.map(worker_patch, range(20)))
    assert all(s == 200 for s in statuses)

    custom = client.get(f"/api/v1/objects/{oid}/metadata").json()["custom"]
    assert custom["init"] == "0"
    for i in range(20):
        assert custom[f"k{i}"] == f"v{i}"


# --------------------------------------------------------------- listing & pagination
def test_list_objects_only_committed_and_stable_pagination(env):
    client, _ = env
    now = time.time()
    # Insert 5 committed objects with deterministic timestamps (2 sharing created_at to test tie-break)
    for idx, (oid, ts) in enumerate([
        ("id-b", now + 1),
        ("id-a", now + 1),  # same created_at as id-b -> id-a must sort before id-b
        ("id-c", now + 2),
        ("id-d", now + 3),
        ("id-e", now + 4),
    ]):
        db.create_object_pending(oid, f"f{idx}.txt", idx + 1, 0, ts)
        db.commit_object(oid, ts, "sum")

    # Add PENDING and DELETING objects that must NOT appear
    db.create_object_pending("hidden-pending", "p", 1, 0, now + 10)
    db.create_object_pending("hidden-deleting", "d", 1, 0, now + 10)
    db.commit_object("hidden-deleting", now + 10, "sum")
    db.set_object_status("hidden-deleting", "DELETING", now + 10)

    expected_order = ["id-e", "id-d", "id-c", "id-a", "id-b"]

    p1 = client.get("/api/v1/objects?limit=2&offset=0").json()
    p2 = client.get("/api/v1/objects?limit=2&offset=2").json()
    p3 = client.get("/api/v1/objects?limit=2&offset=4").json()

    assert (p1["total"], p1["limit"], p1["offset"]) == (5, 2, 0)
    collected = [o["object_id"] for o in p1["objects"] + p2["objects"] + p3["objects"]]
    assert collected == expected_order


@pytest.mark.parametrize("qs", ["limit=-1", "offset=-1", "limit=0", "limit=201", "limit=abc", "offset=xyz"])
def test_list_objects_invalid_pagination(env, qs):
    client, _ = env
    assert_error(client.get(f"/api/v1/objects?{qs}"), 400, "INVALID_PAGINATION")


# --------------------------------------------------------------- delete lifecycle
def test_delete_during_pending_returns_409(env):
    client, _ = env
    db.create_object_pending("7c1f0a5e-2b3d-4e6f-8a9b-0c1d2e3f4a5b", "busy.bin", 10, 1, time.time())
    assert_error(client.delete("/api/v1/objects/7c1f0a5e-2b3d-4e6f-8a9b-0c1d2e3f4a5b"), 409, "OBJECT_BUSY")


def test_delete_happy_path_and_idempotent_second_delete(env):
    client, cluster = env
    obj = upload(client, "bye.bin", b"goodbye", {"k": "v"})
    oid = obj["object_id"]

    r1 = client.delete(f"/api/v1/objects/{oid}")
    assert r1.status_code == 204
    assert all(store == {} for store in cluster.stores.values())
    with db.connection() as c:
        for t in ("objects", "chunks", "replicas", "custom_metadata"):
            assert c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] == 0

    # Second DELETE -> 404 OBJECT_NOT_FOUND
    assert_error(client.delete(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    assert_error(client.get(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    assert_error(client.get(f"/api/v1/objects/{oid}/metadata"), 404, "OBJECT_NOT_FOUND")


def test_delete_with_dead_worker_returns_204_and_defers_pending_delete(env):
    client, cluster = env
    obj = upload(client, "partial.bin", b"partial-data", {"k": "v"})
    oid = obj["object_id"]
    chunk = db.get_object(oid)["chunks"][0]
    w1, w2 = [rep["worker_id"] for rep in chunk["replicas"]]

    # Make w2 unreachable during DELETE
    cluster.dead_for_delete.add(w2)

    r = client.delete(f"/api/v1/objects/{oid}")
    assert r.status_code == 204

    # Live worker chunk was deleted; dead worker retains file until reconciler runs
    assert chunk["chunk_id"] not in cluster.stores[w1]
    assert chunk["chunk_id"] in cluster.stores[w2]

    # Object is immediately invisible everywhere, including second DELETE
    assert_error(client.get(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    assert_error(client.get(f"/api/v1/objects/{oid}/metadata"), 404, "OBJECT_NOT_FOUND")
    assert_error(client.patch(f"/api/v1/objects/{oid}/metadata", json={"x": "1"}), 404, "OBJECT_NOT_FOUND")
    assert_error(client.delete(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    assert client.get("/api/v1/objects").json()["total"] == 0

    # DB retains DELETING object + PENDING_DELETE replica for w2 only
    obj_after = db.get_object(oid)
    assert obj_after["status"] == "DELETING"
    assert obj_after["chunks"][0]["replicas"] == [{"worker_id": w2, "state": "PENDING_DELETE"}]

