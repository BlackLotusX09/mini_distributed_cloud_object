import asyncio
import logging
import random

import pytest

import worker_client
from replication import ChunkStoreFailed, store_chunk
from worker_client import WorkerError, WorkerRejected, WorkerUnreachable


def make_workers(n=4):
    return [
        {"worker_id": f"worker{i}", "address": f"http://worker{i}:8000", "status": "ALIVE", "chunk_count": 0}
        for i in range(1, n + 1)
    ]


def run(coro):
    return asyncio.run(coro)


class ScriptedClient:
    def __init__(self, fail_workers=(), fail_once=()):
        self.fail_workers = set(fail_workers)
        self.fail_once = set(fail_once)
        self.calls = []  # [(worker_id, chunk_id, checksum)]

    async def put_chunk(self, worker, chunk_id, data, checksum):
        wid = worker["worker_id"]
        self.calls.append((wid, chunk_id, checksum))
        if wid in self.fail_once:
            self.fail_once.remove(wid)
            raise WorkerUnreachable(wid, "transient failure")
        if wid in self.fail_workers:
            raise WorkerError(wid, "HTTP 500", 500)


def test_both_workers_ack_on_first_attempt(caplog):
    caplog.set_level(logging.INFO, logger="controller.replication")
    client = ScriptedClient()
    chosen = run(store_chunk("obj_000000", b"payload", "sum0", make_workers(4), client, rng=random.Random(1)))
    assert len(chosen) == 2
    assert len(set(chosen)) == 2
    assert len(client.calls) == 2
    assert all("obj_000000" in m and "outcome=ACK" in m for m in caplog.messages)


def test_one_fails_retries_only_missing_on_distinct_third_worker(caplog):
    caplog.set_level(logging.INFO, logger="controller.replication")
    workers = [
        {"worker_id": "worker1", "address": "http://worker1:8000", "status": "ALIVE", "chunk_count": 0},
        {"worker_id": "worker2", "address": "http://worker2:8000", "status": "ALIVE", "chunk_count": 0},
        {"worker_id": "worker3", "address": "http://worker3:8000", "status": "ALIVE", "chunk_count": 5},
    ]
    client = ScriptedClient(fail_workers={"worker2"})
    chosen = run(store_chunk("chunk_1", b"abc", "sum1", workers, client, rng=random.Random(7)))

    assert set(chosen) == {"worker1", "worker3"}
    assert len(set(chosen)) == 2
    # worker1 was NOT re-sent on the retry round; only 3 total PUTs occurred
    called_wids = [wid for wid, _, _ in client.calls]
    assert called_wids.count("worker1") == 1
    assert called_wids.count("worker2") == 1  # failed worker never retried for this chunk
    assert called_wids.count("worker3") == 1
    assert any("worker=worker2 attempt=0 outcome=FAILED" in m for m in caplog.messages)
    assert any("worker=worker3 attempt=1 outcome=ACK" in m for m in caplog.messages)


def test_both_initial_fail_retries_remaining_workers():
    workers = [
        {"worker_id": "worker1", "chunk_count": 0},
        {"worker_id": "worker2", "chunk_count": 0},
        {"worker_id": "worker3", "chunk_count": 5},
        {"worker_id": "worker4", "chunk_count": 5},
    ]
    client = ScriptedClient(fail_workers={"worker1", "worker2"})
    chosen = run(store_chunk("chunk_2", b"abc", "sum2", workers, client))
    assert set(chosen) == {"worker3", "worker4"}
    assert len(client.calls) == 4


def test_always_failing_workers_exhaust_and_raise():
    client = ScriptedClient(fail_workers={"worker1", "worker2", "worker3", "worker4"})
    with pytest.raises(ChunkStoreFailed) as exc:
        run(store_chunk("chunk_3", b"abc", "sum3", make_workers(4), client, max_retries=3))
    assert exc.value.acked == []


def test_retry_budget_exhausted_reports_acked():
    workers = [
        {"worker_id": "w1", "chunk_count": 0},
        {"worker_id": "w2", "chunk_count": 0},
        {"worker_id": "w3", "chunk_count": 10},
        {"worker_id": "w4", "chunk_count": 10},
        {"worker_id": "w5", "chunk_count": 10},
    ]
    client = ScriptedClient(fail_workers={"w2", "w3", "w4", "w5"})
    with pytest.raises(ChunkStoreFailed) as exc:
        run(store_chunk("chunk_4", b"abc", "sum4", workers, client, max_retries=1))
    assert exc.value.acked == ["w1"]


@pytest.mark.parametrize("n_alive", [0, 1])
def test_fewer_than_two_alive_workers_raises(n_alive):
    client = ScriptedClient()
    with pytest.raises(ChunkStoreFailed):
        run(store_chunk("chunk_5", b"abc", "sum5", make_workers(n_alive), client))
    assert client.calls == []


def test_distinctness_invariant_under_random_failures():
    rng = random.Random(99)
    for i in range(50):
        fail_set = set(rng.sample(["worker1", "worker2", "worker3", "worker4"], k=rng.randint(0, 2)))
        client = ScriptedClient(fail_workers=fail_set)
        chosen = run(store_chunk(f"c_{i}", b"x", "s", make_workers(4), client, rng=rng))
        assert len(chosen) == 2
        assert len(set(chosen)) == 2
        assert not (set(chosen) & fail_set)
        called = [wid for wid, _, _ in client.calls]
        assert len(called) == len(set(called))  # no worker ever called twice for the same chunk


def test_store_chunk_default_signature_uses_module_client(monkeypatch):
    calls = []

    async def fake_put(worker, chunk_id, data, checksum):
        calls.append((worker["worker_id"], worker["address"], chunk_id))

    monkeypatch.setattr(worker_client, "put_chunk", fake_put)
    workers = [{"worker_id": "worker1"}, {"worker_id": "worker2"}]
    chosen = run(store_chunk("chunk_default", b"xyz", "sum", workers))
    assert set(chosen) == {"worker1", "worker2"}
    assert {addr for _, addr, _ in calls} == {"http://worker1:8000", "http://worker2:8000"}


def test_worker_rejected_also_triggers_distinct_retry():
    workers = [
        {"worker_id": "w1", "chunk_count": 0},
        {"worker_id": "w2", "chunk_count": 0},
        {"worker_id": "w3", "chunk_count": 5},
    ]

    class RejectW2:
        async def put_chunk(self, worker, chunk_id, data, checksum):
            if worker["worker_id"] == "w2":
                raise WorkerRejected("w2", "HTTP 400", 400)

    chosen = run(store_chunk("chunk_rej", b"d", "s", workers, RejectW2()))
    assert set(chosen) == {"w1", "w3"}
