import os
import sqlite3
import sys
import threading

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "controller"))

import db  # noqa: E402


@pytest.fixture
def dbpath(tmp_path):
    path = str(tmp_path / "controller.db")
    db.init_db(path)
    return path


def seed_worker(wid="worker1", now=100.0):
    db.upsert_worker(wid, f"http://{wid}:8000", now)


def seed_object(oid="o1", chunks=2):
    db.create_object_pending(oid, "f.bin", 10, chunks, 1.0)


def count(table):
    with db.connection() as c:
        return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ------------------------------------------------------------- connection
def test_pragmas_on_every_connection(dbpath):
    with db.connection() as c:
        assert c.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert c.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert c.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_each_call_gets_its_own_connection(dbpath):
    a, b = db.connect(), db.connect()
    try:
        assert a is not b
    finally:
        a.close()
        b.close()


def test_init_db_is_idempotent(dbpath):
    seed_worker()
    db.init_db()
    db.init_db()
    assert count("workers") == 1


def test_tables_and_indexes_exist(dbpath):
    with db.connection() as c:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        indexes = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"workers", "objects", "chunks", "replicas", "custom_metadata"} <= tables
    assert {"idx_replicas_state", "idx_replicas_worker", "idx_chunks_object_idx"} <= indexes


# ---------------------------------------------------------------- workers
def test_upsert_same_worker_twice_keeps_one_row_and_updates(dbpath):
    assert db.upsert_worker("worker1", "http://old:8000", 100.0) == "new"
    assert db.upsert_worker("worker1", "http://new:8000", 200.0) == "returning"
    assert count("workers") == 1
    w = db.get_worker("worker1")
    assert w["address"] == "http://new:8000"
    assert w["last_heartbeat"] == 200.0
    assert w["registered_at"] == 100.0  # first registration time is kept
    assert w["status"] == "ALIVE" and w["dead_since"] is None


def test_upsert_revives_dead_worker(dbpath):
    seed_worker()
    assert db.mark_dead("worker1", 150.0)
    assert db.upsert_worker("worker1", "http://worker1:8000", 160.0) == "returning"
    w = db.get_worker("worker1")
    assert (w["status"], w["dead_since"]) == ("ALIVE", None)


def test_heartbeat_unknown_returns_none(dbpath):
    assert db.record_heartbeat("ghost", 1.0) is None


def test_heartbeat_updates_and_revives(dbpath):
    seed_worker(now=100.0)
    assert db.record_heartbeat("worker1", 105.0) == "ALIVE"
    assert db.get_worker("worker1")["last_heartbeat"] == 105.0
    db.mark_dead("worker1", 130.0)
    assert db.record_heartbeat("worker1", 140.0) == "DEAD"
    w = db.get_worker("worker1")
    assert (w["status"], w["dead_since"], w["last_heartbeat"]) == ("ALIVE", None, 140.0)


def test_mark_dead_and_alive_transitions(dbpath):
    seed_worker()
    assert db.mark_alive("worker1", 101.0) is False  # already ALIVE
    assert db.mark_dead("worker1", 120.0) is True
    assert db.mark_dead("worker1", 121.0) is False  # already DEAD
    assert db.get_worker("worker1")["dead_since"] == 120.0
    assert db.mark_alive("worker1", 130.0) is True
    assert db.get_worker("worker1")["dead_since"] is None


def test_mark_dead_respects_stale_before(dbpath):
    seed_worker(now=100.0)
    db.record_heartbeat("worker1", 110.0)
    assert db.mark_dead("worker1", 120.0, stale_before=105.0) is False  # fresh heartbeat wins
    assert db.get_worker("worker1")["status"] == "ALIVE"
    assert db.mark_dead("worker1", 120.0, stale_before=111.0) is True


def test_list_workers_filter(dbpath):
    seed_worker("worker1")
    seed_worker("worker2")
    db.mark_dead("worker2", 150.0)
    assert [w["worker_id"] for w in db.list_workers()] == ["worker1", "worker2"]
    assert [w["worker_id"] for w in db.list_workers("ALIVE")] == ["worker1"]
    assert [w["worker_id"] for w in db.list_workers("DEAD")] == ["worker2"]


def test_check_constraint_rejects_bad_status(dbpath):
    with db.connection() as c, pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO workers VALUES ('w','a','ACTIVE',1,NULL,1)")


# ------------------------------------------------- required behaviours
def test_foreign_key_chunk_requires_object(dbpath):
    with pytest.raises(sqlite3.IntegrityError):
        db.add_chunk("c1", "no-such-object", 0, 10)
    assert count("chunks") == 0


def test_foreign_key_replica_requires_chunk_and_worker(dbpath):
    seed_worker()
    seed_object()
    with pytest.raises(sqlite3.IntegrityError):
        db.add_replica("no-chunk", "worker1")
    db.add_chunk("c1", "o1", 0, 10)
    with pytest.raises(sqlite3.IntegrityError):
        db.add_replica("c1", "no-worker")


def test_replica_composite_primary_key(dbpath):
    seed_worker()
    seed_object()
    db.add_chunk("c1", "o1", 0, 10)
    db.add_replica("c1", "worker1")
    with pytest.raises(sqlite3.IntegrityError):
        db.add_replica("c1", "worker1")
    assert count("replicas") == 1


def test_chunk_index_unique_per_object(dbpath):
    seed_object()
    db.add_chunk("c1", "o1", 0, 10)
    with pytest.raises(sqlite3.IntegrityError):
        db.add_chunk("c2", "o1", 0, 10)


def test_transaction_rollback_leaves_no_partial_rows(dbpath):
    seed_worker()
    with pytest.raises(RuntimeError):
        with db.connection() as c, db.transaction(c):
            db.create_object_pending("o1", "f", 10, 2, 1.0, conn=c)
            db.add_chunk("c1", "o1", 0, 5, conn=c)
            db.add_replica("c1", "worker1", conn=c)
            raise RuntimeError("boom")
    assert (count("objects"), count("chunks"), count("replicas")) == (0, 0, 0)


def test_transaction_rollback_on_integrity_error_midway(dbpath):
    with pytest.raises(sqlite3.IntegrityError):
        with db.connection() as c, db.transaction(c):
            db.create_object_pending("o1", "f", 10, 2, 1.0, conn=c)
            db.add_chunk("c1", "ghost-object", 0, 5, conn=c)  # FK violation
    assert count("objects") == 0


def test_transaction_commits_on_clean_exit(dbpath):
    seed_worker()
    with db.connection() as c, db.transaction(c):
        db.create_object_pending("o1", "f", 10, 1, 1.0, conn=c)
        db.add_chunk("c1", "o1", 0, 10, conn=c)
        db.add_replica("c1", "worker1", conn=c)
    assert (count("objects"), count("chunks"), count("replicas")) == (1, 1, 1)


def test_delete_object_cascades(dbpath):
    seed_worker()
    seed_object()
    db.add_chunk("c1", "o1", 0, 5)
    db.add_chunk("c2", "o1", 1, 5)
    db.add_replica("c1", "worker1")
    db.add_replica("c2", "worker1")
    with db.connection() as c:
        c.execute("INSERT INTO custom_metadata VALUES ('o1','k','v')")
    assert db.delete_object("o1") is True
    assert db.delete_object("o1") is False
    assert (count("objects"), count("chunks"), count("replicas"), count("custom_metadata")) == (0, 0, 0, 0)
    assert count("workers") == 1  # workers are untouched


def test_close_and_reopen_preserves_records(tmp_path):
    path = str(tmp_path / "persist.db")
    db.init_db(path)
    seed_worker()
    seed_object()
    db.add_chunk("c1", "o1", 0, 10, "abc")
    db.add_replica("c1", "worker1")
    db.commit_object("o1", 2.0)
    db.init_db(path)  # "restart": fresh connections, schema re-applied
    obj = db.get_object("o1")
    assert obj["status"] == "COMMITTED"
    assert obj["chunks"][0]["checksum"] == "abc"
    assert obj["chunks"][0]["replicas"] == [{"worker_id": "worker1", "state": "OK"}]
    assert db.get_worker("worker1")["address"] == "http://worker1:8000"


# ---------------------------------------------------- object skeleton ops
def test_object_lifecycle(dbpath):
    seed_object()
    assert db.get_object("o1")["status"] == "PENDING"
    assert db.commit_object("o1", 5.0, checksum="sum") is True
    assert db.commit_object("o1", 6.0) is False  # no longer PENDING
    o = db.get_object("o1")
    assert (o["status"], o["checksum"], o["modified_at"]) == ("COMMITTED", "sum", 5.0)
    assert db.get_object("missing") is None


def test_get_object_orders_chunks_by_idx(dbpath):
    seed_object(chunks=3)
    for idx in (2, 0, 1):
        db.add_chunk(f"c{idx}", "o1", idx, 1)
    assert [c["idx"] for c in db.get_object("o1")["chunks"]] == [0, 1, 2]


def test_count_chunks_per_worker_only_counts_ok(dbpath):
    for w in ("worker1", "worker2", "worker3"):
        seed_worker(w)
    seed_object(chunks=3)
    for i in range(3):
        db.add_chunk(f"c{i}", "o1", i, 1)
    db.add_replica("c0", "worker1")
    db.add_replica("c1", "worker1")
    db.add_replica("c2", "worker1", "CORRUPT")
    db.add_replica("c0", "worker2")
    db.add_replica("c1", "worker3", "PENDING_DELETE")
    assert db.count_chunks_per_worker() == {"worker1": 2, "worker2": 1}


def test_concurrent_writers_from_threads(dbpath):
    errors = []

    def work(i):
        try:
            db.upsert_worker(f"worker{i}", f"http://worker{i}:8000", float(i))
            for _ in range(20):
                db.record_heartbeat(f"worker{i}", float(i))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert errors == []
    assert count("workers") == 8

