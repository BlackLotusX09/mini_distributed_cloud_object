"""Tests for Day 6 worker rejoin & inventory reconciliation."""
import asyncio
import hashlib
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import db
import main
import reconciliation
import settings as settings_mod
from worker_client import WorkerClient


def run(coro):
    return asyncio.run(coro)


class FakeRejoinCluster:
    """Fake worker cluster supporting chunk listing (with mtime) + delete/put/get."""

    def __init__(self):
        self.stores = {f"worker{i}": {} for i in range(1, 5)}  # {wid: {cid: {"data": b, "checksum": str, "mtime": float}}}
        self.deletes = []
        cluster = self

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            path = request.url.path
            store = cluster.stores[host]

            if path == f"/internal/workers/{host}/chunks" and request.method == "GET":
                chunks = [
                    {"chunk_id": cid, "size": len(entry["data"]), "checksum": entry["checksum"]}
                    for cid, entry in sorted(store.items())
                ]
                mtimes = {cid: entry["mtime"] for cid, entry in store.items()}
                return httpx.Response(200, json={"worker_id": host, "chunks": chunks, "chunk_mtimes": mtimes})

            if path.startswith("/internal/chunks/"):
                cid = path.rsplit("/", 1)[-1]
                if request.method == "PUT":
                    d = bytes(request.content)
                    store[cid] = {
                        "data": d,
                        "checksum": hashlib.sha256(d).hexdigest(),
                        "mtime": time.time(),
                    }
                    return httpx.Response(201, json={"chunk_id": cid})
                if request.method == "GET":
                    if cid not in store:
                        return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND"})
                    return httpx.Response(200, content=store[cid]["data"])
                if request.method == "DELETE":
                    cluster.deletes.append((host, cid))
                    store.pop(cid, None)
                    return httpx.Response(204)

            return httpx.Response(404)

        self.transport = httpx.MockTransport(handler)

    def put_local(self, wid: str, cid: str, data: bytes, checksum: str = None, mtime: float = 1000.0):
        self.stores[wid][cid] = {
            "data": data,
            "checksum": checksum or hashlib.sha256(data).hexdigest(),
            "mtime": mtime,
        }


@pytest.fixture
def env(tmp_path):
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "reconcile.db"),
        orphan_safety_ttl_seconds=600.0,
    )
    db.init_db(cfg.db_path)
    now = 1000.0
    for i in range(1, 5):
        db.upsert_worker(f"worker{i}", f"http://worker{i}:8000", now)
    return cfg, FakeRejoinCluster()


def _seed_chunk(cluster, oid, cid, data, workers, states=None, status="COMMITTED"):
    states = states or ["OK"] * len(workers)
    checksum = hashlib.sha256(data).hexdigest()
    now = 1000.0
    if db.get_object_row(oid) is None:
        db.create_object_pending(oid, f"{oid}.bin", len(data), 1, now, checksum=checksum)
        if status == "COMMITTED":
            db.commit_object(oid, now, checksum=checksum)
        elif status == "DELETING":
            db.commit_object(oid, now, checksum=checksum)
            db.set_object_status(oid, "DELETING", now)
    db.add_chunk(cid, oid, 0, len(data), checksum=checksum)
    for wid, st in zip(workers, states):
        db.add_replica(cid, wid, st)
        cluster.put_local(wid, cid, data, checksum=checksum, mtime=now)
    return checksum


def test_rejoin_normal_restores_ok_and_overreplication_trimmed(env):
    cfg, cluster = env
    # c1 originally on worker1 + worker2, then worker2 died and was repaired onto worker3
    _seed_chunk(cluster, "o1", "c1", b"hello-rejoin", ["worker1", "worker2", "worker3"])

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            trig = asyncio.Event()
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, trigger_repair_event=trig, now=1500.0)
            assert res["restored_ok"] == 1
            assert res["trimmed"] == 1
            assert not trig.is_set()
        finally:
            await wc.close()

    run(scenario())

    reps = db.get_chunk_replicas_detail("c1")
    assert len(reps) == 2
    assert all(r["state"] == "OK" for r in reps)


def test_rejoin_corrupt_checksum_marks_corrupt_and_triggers_repair(env):
    cfg, cluster = env
    _seed_chunk(cluster, "o1", "c1", b"good-payload", ["worker1", "worker2"])
    # worker2 comes back reporting a mismatched checksum on disk
    cluster.put_local("worker2", "c1", b"bad-payload", checksum="f" * 64)

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            trig = asyncio.Event()
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, trigger_repair_event=trig, now=1500.0)
            assert res["marked_corrupt"] == 1
            assert trig.is_set()
        finally:
            await wc.close()

    run(scenario())

    reps = {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail("c1")}
    assert reps == {"worker1": "OK", "worker2": "CORRUPT"}


def test_rejoin_unrecorded_copy_adopted_when_under_replicated_or_deleted_when_redundant(env):
    cfg, cluster = env
    # c-under has only 1 valid replica on worker1; worker2 holds unrecorded valid copy -> adopt!
    _seed_chunk(cluster, "o1", "c-under", b"under-data", ["worker1"])
    cluster.put_local("worker2", "c-under", b"under-data")

    # c-full already has 2 valid replicas (worker1, worker3); worker2 holds extra unrecorded copy -> delete!
    _seed_chunk(cluster, "o2", "c-full", b"full-data", ["worker1", "worker3"])
    cluster.put_local("worker2", "c-full", b"full-data")

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, now=1500.0)
            assert res["adopted"] == 1
            assert res["deleted_redundant"] == 1
        finally:
            await wc.close()

    run(scenario())

    assert {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail("c-under")} == {
        "worker1": "OK", "worker2": "OK"
    }
    assert "c-full" not in cluster.stores["worker2"]
    assert {r["worker_id"] for r in db.get_chunk_replicas_detail("c-full")} == {"worker1", "worker3"}


def test_rejoin_clears_pending_delete_and_deleting_object_chunks(env):
    cfg, cluster = env
    # PENDING_DELETE replica on a DELETING object
    _seed_chunk(
        cluster, "o-del", "c-del", b"del-bytes", ["worker2"], ["PENDING_DELETE"], status="DELETING"
    )
    # Chunk of DELETING object whose replica wasn't PENDING_DELETE yet
    _seed_chunk(
        cluster, "o-del2", "c-del2", b"del2-bytes", ["worker2"], ["OK"], status="DELETING"
    )
    # Chunk whose object row was completely deleted
    cluster.put_local("worker2", "deleted-obj_000000", b"stale", mtime=100.0)

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, now=2000.0)
            assert res["pending_delete_cleared"] == 1
            assert res["deleted_stale"] == 1
            assert res["deleted_orphan"] == 1
        finally:
            await wc.close()

    run(scenario())

    assert cluster.stores["worker2"] == {}
    assert db.get_object_row("o-del") is None
    assert db.get_object_row("o-del2") is None


def test_rejoin_missing_on_disk_removes_db_row_and_triggers_repair(env):
    cfg, cluster = env
    _seed_chunk(cluster, "o1", "c1", b"missing-disk", ["worker1", "worker2"])
    # Simulate worker2 lost its disk contents while dead
    cluster.stores["worker2"].clear()

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            trig = asyncio.Event()
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, trigger_repair_event=trig, now=1500.0)
            assert res["removed_missing_db_rows"] == 1
            assert trig.is_set()
        finally:
            await wc.close()

    run(scenario())

    assert [r["worker_id"] for r in db.get_chunk_replicas_detail("c1")] == ["worker1"]


def test_rejoin_preserves_pending_object_and_recent_orphan_ttl(env):
    cfg, cluster = env
    now = 2000.0
    # 1. Recorded chunk of active PENDING object
    _seed_chunk(cluster, "o-pend", "o-pend_000000", b"pending1", ["worker2"], status="PENDING")
    # 2. In-flight chunk not yet committed in chunks table, but parent object is PENDING
    db.create_object_pending("o-inflight", "in.bin", 0, 0, now - 10)
    cluster.put_local("worker2", "o-inflight_000000", b"inflight", mtime=100.0)
    # 3. Unknown orphan created recently (inside 600s safety TTL) -> preserved
    cluster.put_local("worker2", "orphan-fresh_000000", b"fresh", mtime=now - 100.0)
    # 4. Unknown orphan older than 600s safety TTL -> deleted
    cluster.put_local("worker2", "orphan-old_000000", b"old", mtime=now - 700.0)

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await reconciliation.reconcile_worker("worker2", wc, cfg, now=now)
            assert res["preserved_pending"] == 2
            assert res["preserved_orphan_ttl"] == 1
            assert res["deleted_orphan"] == 1
        finally:
            await wc.close()

    run(scenario())

    assert "o-pend_000000" in cluster.stores["worker2"]
    assert "o-inflight_000000" in cluster.stores["worker2"]
    assert "orphan-fresh_000000" in cluster.stores["worker2"]
    assert "orphan-old_000000" not in cluster.stores["worker2"]


def test_register_returns_200_immediately_and_reconciles_async(tmp_path):
    cluster = FakeRejoinCluster()
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "reg.db"),
        liveness_check_interval=60.0,
        repair_interval=60.0,
        scrub_interval=600.0,
    )
    app = main.create_app(cfg, worker_transport=cluster.transport)
    with TestClient(app) as client:
        # Register workers 1..4
        for i in range(1, 5):
            r = client.post(
                "/internal/workers/register",
                json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"},
            )
            assert r.status_code == 200

        # Seed a PENDING_DELETE replica on worker2 while it is down
        _seed_chunk(cluster, "o1", "c-pd", b"pd-data", ["worker2"], ["PENDING_DELETE"], status="DELETING")
        assert "c-pd" in cluster.stores["worker2"]

        # Re-register worker2 -> returns 200 immediately, background task deletes c-pd
        r2 = client.post(
            "/internal/workers/register",
            json={"worker_id": "worker2", "address": "http://worker2:8000"},
        )
        assert r2.status_code == 200

        for _ in range(50):
            if "c-pd" not in cluster.stores["worker2"] and db.get_object_row("o1") is None:
                break
            time.sleep(0.02)

        assert "c-pd" not in cluster.stores["worker2"]
        assert db.get_object_row("o1") is None
