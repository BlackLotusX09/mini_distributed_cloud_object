"""Tests for Day 6 replication queries and background repair loop."""
import asyncio
import hashlib
import logging
import time

import httpx
import pytest

import db
import repair
import settings as settings_mod
from worker_client import WorkerClient


def run(coro):
    return asyncio.run(coro)


class FakeWorkers:
    """In-memory worker cluster for testing repair and trimming."""

    def __init__(self):
        self.stores = {f"worker{i}": {} for i in range(1, 5)}
        self.get_overrides = {}    # (wid, cid) or wid -> bytes | int | "unreachable"
        self.put_overrides = {}    # (wid, cid) or wid -> int | "unreachable"
        self.delete_overrides = {} # (wid, cid) or wid -> int | "unreachable"
        self.puts = []
        self.deletes = []
        cluster = self

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            cid = request.url.path.rsplit("/", 1)[-1]
            store = cluster.stores[host]

            if request.method == "GET":
                ov = cluster.get_overrides.get((host, cid), cluster.get_overrides.get(host))
                if ov == "unreachable":
                    raise httpx.ConnectError("down", request=request)
                if isinstance(ov, int):
                    return httpx.Response(ov, json={"error": "INJECTED"})
                if isinstance(ov, bytes):
                    return httpx.Response(200, content=ov)
                if cid not in store:
                    return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND"})
                return httpx.Response(200, content=store[cid])

            if request.method == "PUT":
                cluster.puts.append((host, cid))
                ov = cluster.put_overrides.get((host, cid), cluster.put_overrides.get(host))
                if ov == "unreachable":
                    raise httpx.ConnectError("down", request=request)
                if isinstance(ov, int):
                    return httpx.Response(ov, json={"error": "INJECTED"})
                store[cid] = bytes(request.content)
                return httpx.Response(201, json={"chunk_id": cid})

            if request.method == "DELETE":
                cluster.deletes.append((host, cid))
                ov = cluster.delete_overrides.get((host, cid), cluster.delete_overrides.get(host))
                if ov == "unreachable":
                    raise httpx.ConnectError("down", request=request)
                if isinstance(ov, int):
                    return httpx.Response(ov, json={"error": "INJECTED"})
                store.pop(cid, None)
                return httpx.Response(204)

            return httpx.Response(405)

        self.transport = httpx.MockTransport(handler)


@pytest.fixture
def env(tmp_path):
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "repair.db"),
        repair_grace_period=30.0,
        max_repairs_per_cycle=20,
        repair_concurrency=4,
        max_chunk_repair_attempts=5,
    )
    db.init_db(cfg.db_path)
    now = 1000.0
    for i in range(1, 5):
        db.upsert_worker(f"worker{i}", f"http://worker{i}:8000", now)
    cluster = FakeWorkers()
    return cfg, cluster


def _seed_committed_chunk(cluster, oid, cid, data, workers, states=None, idx=0):
    states = states or ["OK"] * len(workers)
    checksum = hashlib.sha256(data).hexdigest()
    now = 1000.0
    if db.get_object_row(oid) is None:
        db.create_object_pending(oid, f"{oid}.bin", len(data), 1, now, checksum=checksum)
        db.commit_object(oid, now, checksum=checksum)
    db.add_chunk(cid, oid, idx, len(data), checksum=checksum)
    for wid, st in zip(workers, states):
        db.add_replica(cid, wid, st)
        cluster.stores[wid][cid] = data
    return checksum


# ------------------------------------------------------- replication queries
def test_replication_status_categories(env):
    cfg, cluster = env
    now = 1000.0

    # Mark worker4 DEAD (keep worker1, worker2, worker3 ALIVE so c-over has 3 ALIVE workers)
    db.mark_dead("worker4", now)

    # 1. healthy: 2 valid (ALIVE + OK)
    _seed_committed_chunk(cluster, "obj-h", "c-healthy", b"healthy", ["worker1", "worker2"])

    # 2. under_replicated: 1 valid + 1 DEAD
    _seed_committed_chunk(cluster, "obj-u1", "c-under-dead", b"u1", ["worker1", "worker4"])

    # 3. under_replicated: 1 valid + 1 CORRUPT (CORRUPT excluded from valid count)
    _seed_committed_chunk(
        cluster, "obj-u2", "c-under-corrupt", b"u2", ["worker1", "worker2"], ["OK", "CORRUPT"]
    )

    # 4. under_replicated: 1 valid + 1 PENDING_DELETE (PENDING_DELETE excluded)
    _seed_committed_chunk(
        cluster, "obj-u3", "c-under-pd", b"u3", ["worker1", "worker3"], ["OK", "PENDING_DELETE"]
    )

    # 5. over_replicated: 3 valid (all 3 workers are ALIVE)
    _seed_committed_chunk(cluster, "obj-o", "c-over", b"over", ["worker1", "worker2", "worker3"])

    # 6. unavailable: 0 valid, has DEAD worker(s) with OK replica
    cluster.stores["worker5"] = {}
    db.upsert_worker("worker5", "http://worker5:8000", now)
    db.mark_dead("worker5", now)
    _seed_committed_chunk(cluster, "obj-unavail", "c-unavail", b"unavail", ["worker4", "worker5"])

    # 7. unrecoverable: 0 valid, all known replicas CORRUPT
    _seed_committed_chunk(
        cluster, "obj-unrec", "c-unrec", b"unrec", ["worker1", "worker2"], ["CORRUPT", "CORRUPT"]
    )

    # 8. PENDING and DELETING objects must be excluded completely
    db.create_object_pending("obj-pend", "pend.bin", 4, 1, now)
    db.add_chunk("c-pend", "obj-pend", 0, 4, checksum="0" * 64)
    db.add_replica("c-pend", "worker1", "OK")

    db.create_object_pending("obj-del", "del.bin", 4, 1, now)
    db.commit_object("obj-del", now, "0" * 64)
    db.add_chunk("c-del", "obj-del", 0, 4, checksum="0" * 64)
    db.add_replica("c-del", "worker1", "OK")
    db.set_object_status("obj-del", "DELETING", now)

    status = db.get_chunk_replica_status(replication_factor=cfg.replication_factor)
    assert [c["chunk_id"] for c in status["healthy"]] == ["c-healthy"]
    assert [c["chunk_id"] for c in status["under_replicated"]] == [
        "c-under-corrupt", "c-under-dead", "c-under-pd"
    ]
    assert [c["chunk_id"] for c in status["over_replicated"]] == ["c-over"]
    assert [c["chunk_id"] for c in status["unavailable"]] == ["c-unavail"]
    assert [c["chunk_id"] for c in status["unrecoverable"]] == ["c-unrec"]

    # DEAD worker replica rows must remain intact in DB
    unavail_reps = db.get_chunk_replicas_detail("c-unavail")
    assert {(r["worker_id"], r["state"]) for r in unavail_reps} == {
        ("worker4", "OK"), ("worker5", "OK")
    }


# -------------------------------------------------------------- repair loop
def test_grace_period_enforced_for_dead_worker(env):
    cfg, cluster = env
    dead_at = 2000.0
    db.mark_dead("worker2", dead_at)
    _seed_committed_chunk(cluster, "o1", "c1", b"payload-1", ["worker1", "worker2"])

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            # Inside 30s grace window: must NOT repair
            res1 = await repair.repair_cycle(cfg, wc, now=dead_at + 15.0)
            assert res1["under_replicated"] == 1
            assert res1["eligible"] == 0
            assert res1["repaired"] == 0
            assert len(db.get_chunk_replicas_detail("c1")) == 2

            # At 30s grace boundary: MUST repair onto worker3 or worker4
            res2 = await repair.repair_cycle(cfg, wc, now=dead_at + 30.0)
            assert res2["repaired"] == 1
        finally:
            await wc.close()

    run(scenario())

    reps = db.get_chunk_replicas_detail("c1")
    valid = [r for r in reps if r["state"] == "OK" and r["worker_status"] == "ALIVE"]
    assert len(valid) == 2
    assert len({r["worker_id"] for r in reps}) == 3  # worker1, worker2 (DEAD), and new target
    new_target = next(r["worker_id"] for r in valid if r["worker_id"] != "worker1")
    assert cluster.stores[new_target]["c1"] == b"payload-1"


def test_corrupt_replica_repairs_immediately_and_deletes_corrupt_copy(env, caplog):
    caplog.set_level(logging.INFO, logger="controller.repair")
    cfg, cluster = env
    _seed_committed_chunk(
        cluster, "o1", "c1", b"good-bytes", ["worker1", "worker2"], ["OK", "CORRUPT"]
    )
    cluster.stores["worker2"]["c1"] = b"corrupt-bytes"

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            # Should ignore grace period completely and repair immediately
            res = await repair.repair_cycle(cfg, wc, now=1000.0)
            assert res["repaired"] == 1
        finally:
            await wc.close()

    run(scenario())

    reps = db.get_chunk_replicas_detail("c1")
    assert len(reps) == 2
    assert all(r["state"] == "OK" for r in reps)
    wids = {r["worker_id"] for r in reps}
    assert "worker1" in wids
    assert "worker2" not in wids  # CORRUPT worker2 was excluded and deleted
    assert "c1" not in cluster.stores["worker2"]
    assert any("repaired chunk c1: source worker1 -> target " in m for m in caplog.messages)


def test_corrupt_replica_on_dead_worker_marked_pending_delete(env):
    cfg, cluster = env
    db.mark_dead("worker2", 1000.0)
    _seed_committed_chunk(
        cluster, "o1", "c1", b"good-bytes", ["worker1", "worker2"], ["OK", "CORRUPT"]
    )

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await repair.repair_cycle(cfg, wc, now=1000.0)
            assert res["repaired"] == 1
        finally:
            await wc.close()

    run(scenario())

    reps = {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail("c1")}
    assert reps["worker2"] == "PENDING_DELETE"
    assert sum(1 for s in reps.values() if s == "OK") == 3 or len(
        [r for r in db.get_chunk_replicas_detail("c1") if r["state"] == "OK" and r["worker_status"] == "ALIVE"]
    ) == 2


def test_target_distinctness_and_no_eligible_target_logged(env, caplog):
    caplog.set_level(logging.WARNING, logger="controller.repair")
    cfg, cluster = env
    # Only worker1 and worker2 are ALIVE; worker2 holds CORRUPT replica
    db.mark_dead("worker3", 1000.0)
    db.mark_dead("worker4", 1000.0)
    _seed_committed_chunk(
        cluster, "o1", "c1", b"data", ["worker1", "worker2"], ["OK", "CORRUPT"]
    )

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await repair.repair_cycle(cfg, wc, now=1000.0)
            assert res["repaired"] == 0
            assert res["failed"] == 1
        finally:
            await wc.close()

    run(scenario())

    assert any("cannot repair, 2 workers alive" in m for m in caplog.messages)
    # worker2 must remain untouched as CORRUPT (not chosen as target)
    reps = {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail("c1")}
    assert reps == {"worker1": "OK", "worker2": "CORRUPT"}


def test_source_corruption_during_copy_marks_source_corrupt_and_aborts(env):
    cfg, cluster = env
    db.mark_dead("worker2", 900.0)
    _seed_committed_chunk(cluster, "o1", "c1", b"valid-data", ["worker1", "worker2"])
    # Tamper bytes on worker1 (the only valid ALIVE source)
    cluster.get_overrides[("worker1", "c1")] = b"tampered!!"

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            outcome, target = await repair.copy_chunk("c1", wc, replication_factor=2)
            assert outcome == "source_corrupt"
            assert target is None
        finally:
            await wc.close()

    run(scenario())

    reps = {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail("c1")}
    assert reps["worker1"] == "CORRUPT"
    assert cluster.puts == []


def test_repair_cycle_idempotent_on_repeated_runs(env, caplog):
    caplog.set_level(logging.INFO, logger="controller.repair")
    cfg, cluster = env
    _seed_committed_chunk(
        cluster, "o1", "c1", b"idempotent-data", ["worker1", "worker2"], ["OK", "CORRUPT"]
    )

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            r1 = await repair.repair_cycle(cfg, wc, now=1000.0)
            assert r1["repaired"] == 1
            puts_after_first = len(cluster.puts)

            caplog.clear()
            r2 = await repair.repair_cycle(cfg, wc, now=1010.0)
            assert r2 == {
                "under_replicated": 0, "eligible": 0, "repaired": 0,
                "failed": 0, "unavailable": 0, "trimmed": 0, "skipped_backoff": 0,
            }
            assert len(cluster.puts) == puts_after_first
            assert caplog.messages == []  # silent on idle cycles
        finally:
            await wc.close()

    run(scenario())


def test_backoff_guard_after_max_attempts(env, caplog):
    caplog.set_level(logging.WARNING, logger="controller.repair")
    cfg, cluster = env
    db.mark_dead("worker2", 900.0)
    _seed_committed_chunk(cluster, "o1", "c1", b"fail-copy", ["worker1", "worker2"])
    cluster.put_overrides["worker3"] = 500
    cluster.put_overrides["worker4"] = 500

    attempts = {}

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            for _ in range(cfg.max_chunk_repair_attempts):
                res = await repair.repair_cycle(cfg, wc, attempt_counts=attempts, now=1000.0)
                assert res["failed"] == 1
            assert attempts["c1"] == cfg.max_chunk_repair_attempts

            puts_before = len(cluster.puts)
            res_after = await repair.repair_cycle(cfg, wc, attempt_counts=attempts, now=1010.0)
            assert res_after["skipped_backoff"] == 1
            assert res_after["eligible"] == 0
            assert len(cluster.puts) == puts_before  # no further PUT attempts
        finally:
            await wc.close()

    run(scenario())
    assert any("exceeded max repair attempts" in m for m in caplog.messages)


def test_over_replication_trimmed_highest_loaded_worker(env):
    cfg, cluster = env
    # Give worker1 extra chunks so worker1 has the highest load
    _seed_committed_chunk(cluster, "o0", "c0a", b"a", ["worker1", "worker4"], idx=0)
    _seed_committed_chunk(cluster, "o0", "c0b", b"b", ["worker1", "worker4"], idx=1)
    # Make c-over have 3 valid replicas: worker1 (count 3), worker2 (count 1), worker3 (count 1)
    _seed_committed_chunk(cluster, "o1", "c-over", b"over3", ["worker1", "worker2", "worker3"])

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            res = await repair.repair_cycle(cfg, wc, now=1000.0)
            assert res["trimmed"] == 1
        finally:
            await wc.close()

    run(scenario())

    reps = [r["worker_id"] for r in db.get_chunk_replicas_detail("c-over")]
    assert reps == ["worker2", "worker3"]
    assert "c-over" not in cluster.stores["worker1"]


def test_day6_day7_settings_constants():
    assert settings_mod.REPAIR_GRACE_PERIOD == 30.0
    assert settings_mod.REPAIR_INTERVAL == 10.0
    assert settings_mod.MAX_REPAIRS_PER_CYCLE == 20
    assert settings_mod.REPAIR_CONCURRENCY == 4
    assert settings_mod.MAX_CHUNK_REPAIR_ATTEMPTS == 5
    assert settings_mod.ORPHAN_SAFETY_TTL_SECONDS == 600
    assert settings_mod.SCRUB_INTERVAL == 300.0
    assert settings_mod.WORKER_SCRUB_YIELD_DELAY == 0.005

