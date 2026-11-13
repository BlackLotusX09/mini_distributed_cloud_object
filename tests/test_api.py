import logging
import os
import sys
import time
from contextlib import ExitStack

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "controller"))

import db  # noqa: E402
import main  # noqa: E402
import settings as settings_mod  # noqa: E402


@pytest.fixture
def make_client(tmp_path):
    """Factory: make_client(**settings) -> started TestClient (lifespan running)."""
    stack = ExitStack()
    path = str(tmp_path / "controller.db")

    def factory(timeout=15.0, interval=3.0, **kw):
        cfg = settings_mod.Settings(
            db_path=path, heartbeat_timeout=timeout, liveness_check_interval=interval, **kw
        )
        return stack.enter_context(TestClient(main.create_app(cfg)))

    factory.db_path = path
    yield factory
    stack.close()


def reg(client, wid="worker1", address=None, **extra):
    body = {"worker_id": wid, "address": address or f"http://{wid}:8000", **extra}
    return client.post("/internal/workers/register", json=body)


def hb(client, wid="worker1", **extra):
    return client.post("/internal/workers/heartbeat", json={"worker_id": wid, **extra})


def workers(client):
    return {w["worker_id"]: w for w in client.get("/api/v1/admin/workers").json()}


def wait_for(pred, timeout=6.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def assert_error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error", "message"}
    assert body["error"] == code and body["message"]


# --------------------------------------------------------------- settings
def test_settings_defaults():
    s = settings_mod.load({})
    assert (s.db_path, s.heartbeat_timeout, s.liveness_check_interval) == ("/data/controller.db", 15.0, 3.0)
    assert (s.replication_factor, s.chunk_size) == (2, 1048576)


def test_settings_from_env_and_validation():
    s = settings_mod.load({"DB_PATH": "/x.db", "HEARTBEAT_TIMEOUT": "20", "REPLICATION_FACTOR": "3"})
    assert (s.db_path, s.heartbeat_timeout, s.replication_factor) == ("/x.db", 20.0, 3)
    for bad in ({"REPLICATION_FACTOR": "0"}, {"CHUNK_SIZE": "abc"}, {"HEARTBEAT_TIMEOUT": "-1"}):
        with pytest.raises(settings_mod.SettingsError):
            settings_mod.load(bad)


# --------------------------------------------------------------- register
def test_register_new_worker(make_client, caplog):
    caplog.set_level(logging.INFO, logger="controller")
    c = make_client()
    r = reg(c, free_bytes=1000, chunk_count=3)
    assert r.status_code == 200
    assert r.json() == {"status": "registered", "heartbeat_interval": 5}
    w = workers(c)["worker1"]
    assert (w["status"], w["address"], w["dead_since"], w["chunk_count"]) == ("ALIVE", "http://worker1:8000", None, 0)
    assert any("registered (new)" in m for m in caplog.messages)


def test_register_returning_worker_updates_address_and_logs(make_client, caplog):
    caplog.set_level(logging.INFO, logger="controller")
    c = make_client()
    reg(c)
    reg(c, address="http://worker1-moved:9000")
    assert len(workers(c)) == 1
    assert workers(c)["worker1"]["address"] == "http://worker1-moved:9000"
    assert any("registered (returning" in m for m in caplog.messages)


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"address": "http://w:8000"},
        {"worker_id": "w"},
        {"worker_id": "", "address": "http://w:8000"},
        {"worker_id": "   ", "address": "http://w:8000"},
        {"worker_id": 7, "address": "http://w:8000"},
        {"worker_id": "w", "address": "not-a-url"},
        {"worker_id": "w", "address": "http://"},
        {"worker_id": "w", "address": "http://w:8000", "free_bytes": -1},
        {"worker_id": "w", "address": "http://w:8000", "free_bytes": "lots"},
        {"worker_id": "w", "address": "http://w:8000", "chunk_count": True},
        {"worker_id": "w", "address": "http://w:8000", "chunk_count": 1.5},
    ],
)
def test_register_invalid_bodies(make_client, body):
    c = make_client()
    assert_error(c.post("/internal/workers/register", json=body), 400, "INVALID_REQUEST")
    assert workers(c) == {}


def test_register_non_json_and_non_object(make_client):
    c = make_client()
    assert_error(c.post("/internal/workers/register", content=b"not json",
                        headers={"Content-Type": "application/json"}), 400, "INVALID_REQUEST")
    assert_error(c.post("/internal/workers/register", json=["a"]), 400, "INVALID_REQUEST")


# -------------------------------------------------------------- heartbeat
def test_heartbeat_unknown_worker(make_client):
    c = make_client()
    assert_error(hb(c, "ghost"), 404, "UNKNOWN_WORKER")


def test_heartbeat_missing_worker_id(make_client):
    c = make_client()
    assert_error(c.post("/internal/workers/heartbeat", json={}), 400, "INVALID_REQUEST")


def test_heartbeat_ok_and_routine_heartbeats_not_logged(make_client, caplog):
    c = make_client()
    reg(c)
    caplog.clear()
    caplog.set_level(logging.INFO, logger="controller")
    for _ in range(5):
        assert hb(c).status_code == 200
    assert [m for m in caplog.messages if "worker1" in m] == []


def test_worker_supplied_timestamp_is_ignored(make_client):
    c = make_client()
    reg(c)
    time.sleep(0.3)
    r = hb(c, timestamp="1970-01-01T00:00:00+00:00", free_bytes=1, chunk_count=0)
    assert r.status_code == 200
    assert workers(c)["worker1"]["last_heartbeat_age"] < 1.0  # controller clock, not epoch 0


def test_heartbeat_age_grows(make_client):
    c = make_client()
    reg(c)
    time.sleep(0.6)
    assert workers(c)["worker1"]["last_heartbeat_age"] >= 0.5


# -------------------------------------------------------- admin / health
def test_admin_workers_shape_and_chunk_counts(make_client):
    c = make_client()
    reg(c, "worker1")
    reg(c, "worker2")
    db.create_object_pending("o1", "f", 10, 2, 1.0)
    db.add_chunk("c1", "o1", 0, 5)
    db.add_chunk("c2", "o1", 1, 5)
    db.add_replica("c1", "worker1")
    db.add_replica("c2", "worker1")
    db.add_replica("c2", "worker2", "CORRUPT")
    data = c.get("/api/v1/admin/workers").json()
    assert [w["worker_id"] for w in data] == ["worker1", "worker2"]
    assert set(data[0]) == {"worker_id", "address", "status", "last_heartbeat_age", "dead_since", "chunk_count"}
    assert (data[0]["chunk_count"], data[1]["chunk_count"]) == (2, 0)


def test_health(make_client):
    c = make_client()
    assert c.get("/health").json() == {"status": "ok", "role": "controller", "active_workers": 0}
    reg(c)
    assert c.get("/health").json()["active_workers"] == 1


def test_unknown_route_uses_error_format(make_client):
    assert_error(make_client().get("/nope"), 404, "NOT_FOUND")


# -------------------------------------------------------------- placement
def test_placement_test_insufficient_workers(make_client):
    c = make_client()
    r = c.get("/api/v1/admin/placement-test")
    assert_error(r, 503, "INSUFFICIENT_WORKERS")
    assert r.json()["message"] == "need 2 healthy workers, have 0"
    reg(c, "worker1")
    r = c.get("/api/v1/admin/placement-test")
    assert r.status_code == 503 and r.json()["message"] == "need 2 healthy workers, have 1"


def test_placement_test_returns_distinct_alive_workers(make_client):
    c = make_client()
    for i in range(1, 5):
        reg(c, f"worker{i}")
    db.mark_dead("worker3", time.time())
    for _ in range(20):
        body = c.get("/api/v1/admin/placement-test").json()
        assert body["replication_factor"] == 2
        assert len(set(body["workers"])) == 2 and "worker3" not in body["workers"]


def test_placement_test_prefers_least_loaded(make_client):
    c = make_client()
    for i in range(1, 5):
        reg(c, f"worker{i}")
    db.create_object_pending("o1", "f", 10, 4, 1.0)
    for i in range(4):
        db.add_chunk(f"c{i}", "o1", i, 1)
    for i in range(3):  # worker1: 3, worker2: 2, worker3: 1, worker4: 0
        db.add_replica(f"c{i}", "worker1")
    for i in range(2):
        db.add_replica(f"c{i}", "worker2")
    db.add_replica("c0", "worker3")
    for _ in range(10):
        assert set(c.get("/api/v1/admin/placement-test").json()["workers"]) == {"worker3", "worker4"}


def test_replication_factor_setting_is_used(make_client):
    c = make_client(replication_factor=3)
    for i in range(1, 4):
        reg(c, f"worker{i}")
    assert len(c.get("/api/v1/admin/placement-test").json()["workers"]) == 3


# ----------------------------------------------------------- persistence
def test_state_survives_controller_restart(make_client, tmp_path):
    first = make_client()
    reg(first, "worker1", address="http://worker1:8000")
    second = make_client()  # new app instance on the same DB file
    assert workers(second)["worker1"]["address"] == "http://worker1:8000"
    assert hb(second).status_code == 200  # still known: no re-registration needed


# --------------------------------------------------------------- liveness
def test_worker_marked_dead_then_revived_by_heartbeat(make_client, caplog):
    caplog.set_level(logging.INFO, logger="controller")
    c = make_client(timeout=1.0, interval=0.1)
    reg(c, "worker1")
    reg(c, "worker2")
    db.create_object_pending("o1", "f", 10, 1, 1.0)
    db.add_chunk("c1", "o1", 0, 10)
    db.add_replica("c1", "worker1")

    # keep worker2 alive; let worker1 go silent
    def worker1_dead():
        hb(c, "worker2")
        return workers(c)["worker1"]["status"] == "DEAD"

    assert wait_for(worker1_dead)
    w = workers(c)
    assert w["worker2"]["status"] == "ALIVE"
    assert w["worker1"]["dead_since"] is not None
    assert any("worker1 ALIVE -> DEAD (no heartbeat for" in m for m in caplog.messages)
    assert not any("worker2 ALIVE -> DEAD" in m for m in caplog.messages)
    assert db.get_object("o1")["chunks"][0]["replicas"] == [{"worker_id": "worker1", "state": "OK"}]
    assert db.count_chunks_per_worker() == {"worker1": 1}  # replicas untouched when DEAD

    assert hb(c, "worker1").status_code == 200
    assert any("worker1 DEAD -> ALIVE" in m for m in caplog.messages)
    w = workers(c)["worker1"]
    assert (w["status"], w["dead_since"]) == ("ALIVE", None)

    # transition is logged once, not on every sweep
    assert sum("worker1 ALIVE -> DEAD" in m for m in caplog.messages) == 1


def test_dead_worker_can_re_register(make_client):
    c = make_client(timeout=0.5, interval=0.1)
    reg(c)
    assert wait_for(lambda: workers(c)["worker1"]["status"] == "DEAD")
    assert reg(c, address="http://worker1-new:8000").status_code == 200
    w = workers(c)["worker1"]
    assert (w["status"], w["dead_since"], w["address"]) == ("ALIVE", None, "http://worker1-new:8000")


def test_startup_grace_period_prevents_false_dead(tmp_path):
    path = str(tmp_path / "grace.db")
    db.init_db(path)
    db.upsert_worker("worker1", "http://worker1:8000", time.time() - 100)  # very stale
    cfg = settings_mod.Settings(db_path=path, heartbeat_timeout=2.0, liveness_check_interval=0.1)
    with TestClient(main.create_app(cfg)) as c:
        t0 = time.monotonic()
        time.sleep(1.0)  # inside the 2 s grace window
        assert workers(c)["worker1"]["status"] == "ALIVE"
        assert wait_for(lambda: workers(c)["worker1"]["status"] == "DEAD", timeout=5)
        assert time.monotonic() - t0 >= 1.9  # not marked before the grace period ended


def test_grace_period_lets_returning_worker_heartbeat_first(tmp_path):
    path = str(tmp_path / "grace2.db")
    db.init_db(path)
    db.upsert_worker("worker1", "http://worker1:8000", time.time() - 100)
    cfg = settings_mod.Settings(db_path=path, heartbeat_timeout=1.5, liveness_check_interval=0.1)
    with TestClient(main.create_app(cfg)) as c:
        assert hb(c).status_code == 200  # worker's next heartbeat arrives during grace
        time.sleep(0.8)
        hb(c)
        time.sleep(0.8)  # grace over, but heartbeats are fresh
        assert workers(c)["worker1"]["status"] == "ALIVE"


def test_monitor_survives_transient_errors(make_client, monkeypatch):
    real = main.sweep_dead_workers
    calls = {"n": 0}

    def flaky(now, timeout):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise RuntimeError("database is locked")
        return real(now, timeout)

    monkeypatch.setattr(main, "sweep_dead_workers", flaky)
    c = make_client(timeout=0.5, interval=0.1)
    reg(c)
    assert wait_for(lambda: workers(c)["worker1"]["status"] == "DEAD")
    assert calls["n"] > 3


def test_sweep_function_directly(tmp_path):
    db.init_db(str(tmp_path / "sweep.db"))
    db.upsert_worker("a", "http://a:1", 100.0)
    db.upsert_worker("b", "http://b:1", 110.0)
    assert main.sweep_dead_workers(now=114.0, timeout=15.0) == []
    out = main.sweep_dead_workers(now=116.0, timeout=15.0)  # a: 16s, b: 6s
    assert [w for w, _ in out] == ["a"] and out[0][1] == 16.0
    assert main.sweep_dead_workers(now=120.0, timeout=15.0) == []  # already DEAD: no repeat
    assert db.get_worker("a")["dead_since"] == 116.0

