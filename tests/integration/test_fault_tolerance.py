"""Failure & integrity regression suite."""
import contextlib
import time

import pytest

from backends import WORKERS
from helpers import (all_chunks_have, expect, expect_error, get_chunk_map, sha256_hex, valid_workers,
                     wait_for, wait_worker, workers_state)

pytestmark = pytest.mark.integration


def disk_ids(cluster, worker):
    return set(cluster.worker_files(worker))


def replica_rows(cluster, oid):
    return cluster.db_rows("SELECT r.chunk_id, r.worker_id, r.state FROM replicas r JOIN chunks c "
                           "ON c.chunk_id = r.chunk_id WHERE c.object_id = ? ORDER BY 1,2", (oid,))


def consistent(cluster, client, oid, n=2):
    """Chunk map has n valid replicas everywhere and each OK replica row has its file on disk."""
    cm = get_chunk_map(client, oid)
    for c in cm:
        v = valid_workers(c)
        if len(v) != n or any(c["chunk_id"] not in disk_ids(cluster, w) for w in v):
            return False
    return True


# ------------------------------------------------------------- placement
def test_5mb_chunks_on_two_distinct_alive_workers(cluster, client, uploaded_object):
    oid, _, _ = uploaded_object(5 * 1024 * 1024)
    cm = get_chunk_map(client, oid)
    assert len(cm) == 5
    for c in cm:
        ws = valid_workers(c)
        assert len(ws) == 2 and len(set(ws)) == 2
        for w in ws:
            assert c["chunk_id"] in disk_ids(cluster, w), f"{c['chunk_id']} missing on {w}'s disk"


def test_20mb_spreads_over_all_workers(cluster, client, uploaded_object):
    oid, _, _ = uploaded_object(20 * 1024 * 1024)
    used = {w for c in get_chunk_map(client, oid) for w in valid_workers(c)}
    assert used == set(WORKERS), f"expected replicas on all 4 workers, got {sorted(used)}"


# ----------------------------------------------------- death / re-replication
@pytest.mark.slow
def test_worker_death_rereplication_and_rejoin(cluster, client, uploaded_object, stop_worker):
    oid, raw, digest = uploaded_object(6 * 1024 * 1024)
    on_w2 = {c["chunk_id"] for c in get_chunk_map(client, oid) if "worker2" in valid_workers(c)}
    assert on_w2, "placement put no chunk on worker2 (unexpected for 6 chunks across 4 workers)"
    with stop_worker("worker2"):
        r = expect(client.get(f"/api/v1/objects/{oid}"), 200)  # served immediately from survivors
        assert sha256_hex(r.content) == digest
        wait_for(lambda: all_chunks_have(client, oid, 2, exclude=["worker2"]),
                 timeout=cluster.repair_grace + 90, interval=1.0,
                 desc="all chunks back to 2 valid replicas on surviving workers")
        assert sha256_hex(client.get(f"/api/v1/objects/{oid}").content) == digest
    # worker2 restarted by the fixture: reconcile must leave exactly 2 replicas and no stale files
    wait_for(lambda: consistent(cluster, client, oid), timeout=90, interval=1.0, desc="reconciled to 2 replicas")
    rows = replica_rows(cluster, oid)
    expected = {(r["chunk_id"], r["worker_id"]) for r in rows}
    stale = {cid for cid in disk_ids(cluster, "worker2") if cid.startswith(oid) and (cid, "worker2") not in expected}
    assert not stale, f"stale chunk files on worker2: {sorted(stale)}"
    assert sha256_hex(client.get(f"/api/v1/objects/{oid}").content) == digest


def test_fast_flap_creates_no_new_replicas(cluster, client, uploaded_object, stop_worker):
    oid, _, digest = uploaded_object(4 * 1024 * 1024)
    before = replica_rows(cluster, oid)
    with stop_worker("worker2"):
        pass  # restart immediately: far inside REPAIR_GRACE_PERIOD
    time.sleep(cluster.repair_grace / 2 if cluster.name == "local" else 5)
    assert replica_rows(cluster, oid) == before, "flap must not re-replicate"
    assert sha256_hex(client.get(f"/api/v1/objects/{oid}").content) == digest


# ------------------------------------------------------------- capacity loss
@pytest.mark.slow
def test_quorum_loss_rejects_upload_without_pending_rows(cluster, client, stop_worker):
    with contextlib.ExitStack() as st:
        for w in ("worker2", "worker3", "worker4"):
            st.enter_context(stop_worker(w))
        t0 = time.monotonic()
        r = client.post("/api/v1/objects", files={"file": ("x.bin", b"data", "application/octet-stream")})
        expect_error(r, 503, "INSUFFICIENT_WORKERS")
        assert time.monotonic() - t0 < 5, "must fail fast"
        assert cluster.db_rows("SELECT COUNT(*) AS n FROM objects WHERE status='PENDING'")[0]["n"] == 0


# ------------------------------------------------------------- slow worker
@pytest.mark.slow
def test_paused_worker_reads_fall_back_and_liveness_marks_dead(cluster, client, uploaded_object, pause_worker):
    oid, _, digest = uploaded_object(1024 * 1024)
    victim = valid_workers(get_chunk_map(client, oid)[0])[0]
    with pause_worker(victim):
        t0 = time.monotonic()
        for _ in range(4):  # whichever replica is tried first, the read must complete via the healthy one
            r = expect(client.get(f"/api/v1/objects/{oid}"), 200)
            assert sha256_hex(r.content) == digest
        assert time.monotonic() - t0 < 4 * (cluster.read_timeout + 5), "reads hung on the paused worker"
        wait_worker(client, victim, "DEAD", timeout=90)
    assert sha256_hex(client.get(f"/api/v1/objects/{oid}").content) == digest


# -------------------------------------------------------- corruption / scrub
def test_single_corrupt_replica_is_detected_and_replaced(cluster, client, uploaded_object, corrupt_chunk_on_disk):
    oid, raw, digest = uploaded_object(1024 * 1024)
    chunk = get_chunk_map(client, oid)[0]
    bad, good = valid_workers(chunk)
    corrupt_chunk_on_disk(bad, chunk["chunk_id"])
    for _ in range(6):  # reads never return corrupt bytes, via fallback if the bad replica is tried first
        assert sha256_hex(expect(client.get(f"/api/v1/objects/{oid}"), 200).content) == digest
    s = expect(client.post("/api/v1/admin/scrub"), 200).json()
    assert set(s) == {"workers_scrubbed", "chunks_checked", "corrupt_found", "repairs_triggered"}
    wait_for(lambda: all_chunks_have(client, oid, 2, exclude=[bad]), timeout=60, interval=0.5,
             desc="corrupt replica replaced on another worker")
    wait_for(lambda: chunk["chunk_id"] not in disk_ids(cluster, bad), timeout=30,
             desc=f"bad file deleted from {bad}")
    for w in valid_workers(get_chunk_map(client, oid)[0]):
        assert cluster.worker_files(w)[chunk["chunk_id"]]["sha"] == chunk["checksum"]
    assert good in valid_workers(get_chunk_map(client, oid)[0])


def test_both_replicas_corrupt_fails_cleanly(cluster, client, uploaded_object, corrupt_chunk_on_disk):
    oid, raw, digest = uploaded_object(512 * 1024)
    chunk = get_chunk_map(client, oid)[0]
    for w in valid_workers(chunk):
        corrupt_chunk_on_disk(w, chunk["chunk_id"])
    r = client.get(f"/api/v1/objects/{oid}")
    assert r.status_code in (500, 503), f"Expected 500/503, got {r.status_code}"
    assert r.json()["error"] == "DATA_CORRUPT", r.text
    assert r.headers["content-type"].startswith("application/json"), "corrupt bytes must never be streamed"
    assert raw[:64] not in r.content
    rep = expect(client.get("/api/v1/admin/replication"), 200).json()
    assert rep["unrecoverable"] >= 1
    assert client.get(f"/api/v1/admin/objects/{oid}/chunks").status_code == 200  # data preserved, not deleted


# --------------------------------------------------------- delete in outage
@pytest.mark.slow
def test_delete_during_outage_is_reconciled_on_rejoin(cluster, client, uploaded_object, stop_worker):
    oid = cids = None
    for _ in range(5):  # need an object that actually has chunks on worker3
        oid, _, _ = uploaded_object(8 * 1024 * 1024)
        cids = {c["chunk_id"] for c in get_chunk_map(client, oid) if "worker3" in valid_workers(c)}
        if cids:
            break
    assert cids
    with stop_worker("worker3"):
        expect(client.delete(f"/api/v1/objects/{oid}"), 204)
        expect_error(client.get(f"/api/v1/objects/{oid}"), 404, "OBJECT_NOT_FOUND")
    wait_for(lambda: not (cids & disk_ids(cluster, "worker3")), timeout=60, interval=1.0,
             desc="worker3 chunk files for deleted object removed after rejoin")
    wait_for(lambda: not cluster.db_rows("SELECT 1 FROM objects WHERE object_id = ?", (oid,)),
             timeout=60, interval=1.0, desc="object row fully purged")
