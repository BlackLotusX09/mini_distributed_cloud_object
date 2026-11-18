"""Tests for Day 7 two-layer integrity, worker/controller scrubber, and admin observability."""
import asyncio
import hashlib
import os
import time

import httpx
import pytest
from fastapi.testclient import TestClient

import db
import main
import objects_api
import scrubber
import settings as settings_mod
from storage import Storage, sha256_hex


class ScrubbableFakeCluster:
    """In-memory cluster backed by per-worker payloads AND sidecars to simulate silent bit-rot."""

    def __init__(self):
        # {wid: {cid: {"data": bytes, "sidecar": str}}}
        self.stores = {f"worker{i}": {} for i in range(1, 5)}
        self.get_overrides = {}

        cluster = self

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            path = request.url.path
            store = cluster.stores[host]

            if path == "/internal/scrub" and request.method == "POST":
                checked = 0
                ok = 0
                checksums = {}
                corrupt = []
                missing_checksum = []
                for cid, entry in sorted(store.items()):
                    checked += 1
                    fresh = hashlib.sha256(entry["data"]).hexdigest()
                    sidecar = entry.get("sidecar")
                    if sidecar is None:
                        missing_checksum.append(cid)
                    elif sidecar != fresh:
                        corrupt.append(cid)
                    else:
                        ok += 1
                        checksums[cid] = fresh
                return httpx.Response(
                    200,
                    json={
                        "checked": checked,
                        "ok": ok,
                        "checksums": checksums,
                        "corrupt": corrupt,
                        "missing_checksum": missing_checksum,
                    },
                )

            if path == f"/internal/workers/{host}/chunks" and request.method == "GET":
                chunks = [
                    {
                        "chunk_id": cid,
                        "size": len(entry["data"]),
                        "checksum": entry["sidecar"],
                    }
                    for cid, entry in sorted(store.items())
                    if entry.get("sidecar") is not None
                ]
                return httpx.Response(200, json={"worker_id": host, "chunks": chunks, "chunk_mtimes": {}})

            if path.startswith("/internal/chunks/"):
                cid = path.rsplit("/", 1)[-1]
                if request.method == "PUT":
                    d = bytes(request.content)
                    store[cid] = {"data": d, "sidecar": hashlib.sha256(d).hexdigest()}
                    return httpx.Response(201, json={"chunk_id": cid})
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
                    entry = store[cid]
                    fresh = hashlib.sha256(entry["data"]).hexdigest()
                    if entry.get("sidecar") is None:
                        return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND"})
                    if entry["sidecar"] != fresh:
                        return httpx.Response(409, json={"error": "CHUNK_CORRUPT"})
                    return httpx.Response(200, content=entry["data"])
                if request.method == "DELETE":
                    store.pop(cid, None)
                    return httpx.Response(204)

            return httpx.Response(404)

        self.transport = httpx.MockTransport(handler)


@pytest.fixture
def env(tmp_path):
    cluster = ScrubbableFakeCluster()
    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "scrub.db"),
        heartbeat_timeout=15.0,
        liveness_check_interval=60.0,
        repair_interval=60.0,
        repair_grace_period=30.0,
        scrub_interval=300.0,
    )
    app = main.create_app(cfg, worker_transport=cluster.transport)
    with TestClient(app) as client:
        for i in range(1, 5):
            assert client.post(
                "/internal/workers/register",
                json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"},
            ).status_code == 200
        yield client, cluster, app


# ------------------------------------------------------- worker Storage.scrub
def test_worker_storage_scrub_detects_bitrot_and_missing_sidecar(tmp_path):
    s = Storage(str(tmp_path))
    d1 = os.urandom(4096)
    d2 = os.urandom(2_000_000 // 2)  # 1,000,000 bytes
    d3 = os.urandom(2048)
    s.write_chunk("c_ok", d1)
    s.write_chunk("c_rot", d2)
    s.write_chunk("c_nosum", d3)

    # Bit-flip c_rot without reading it
    p_rot = os.path.join(s.root, "c_", "c_rot.chunk")
    raw = bytearray(open(p_rot, "rb").read())
    raw[123] ^= 0xFF
    open(p_rot, "wb").write(bytes(raw))

    # Remove sidecar of c_nosum
    os.unlink(os.path.join(s.root, "c_", "c_nosum.sha256"))

    report = s.scrub(yield_delay=0.0)
    assert report["checked"] == 3
    assert report["ok"] == 1
    assert report["checksums"] == {"c_ok": sha256_hex(d1)}
    assert report["corrupt"] == ["c_rot"]
    assert report["missing_checksum"] == ["c_nosum"]


# --------------------------------------------------- read-path integrity
def test_read_path_fallback_marks_corrupt_and_sets_repair_event(env, monkeypatch):
    client, cluster, _ = env
    payload = b"read-integrity-bytes" * 50
    oid = client.post("/api/v1/objects", files={"file": ("r.bin", payload)}).json()["object_id"]
    ch0 = db.get_object(oid)["chunks"][0]
    w1, w2 = [r["worker_id"] for r in ch0["replicas"]]

    # Force w1 to be tried first deterministically while keeping w2 ALIVE
    monkeypatch.setattr(
        objects_api.random,
        "shuffle",
        lambda lst: lst.sort(key=lambda r: 0 if r["worker_id"] == w1 else 1),
    )
    cluster.get_overrides[(w1, ch0["chunk_id"])] = b"X" * len(payload)

    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200
    assert dl.content == payload

    # Wait briefly for immediate repair triggered automatically by trigger_repair_event
    for _ in range(60):
        cur = db.get_chunk_replicas_detail(ch0["chunk_id"])
        if len([r for r in cur if r["state"] == "OK"]) == 2 and w1 not in {r["worker_id"] for r in cur}:
            break
        time.sleep(0.02)

    cur = db.get_chunk_replicas_detail(ch0["chunk_id"])
    assert len([r for r in cur if r["state"] == "OK"]) == 2
    assert w2 in {r["worker_id"] for r in cur}
    assert w1 not in {r["worker_id"] for r in cur}


def test_read_path_chunk0_both_corrupt_names_object_and_idx(env):
    client, cluster, _ = env
    payload = b"corrupt-preflight"
    oid = client.post("/api/v1/objects", files={"file": ("c0.bin", payload)}).json()["object_id"]
    ch0 = db.get_object(oid)["chunks"][0]
    for r in ch0["replicas"]:
        cluster.get_overrides[(r["worker_id"], ch0["chunk_id"])] = b"bad"

    resp = client.get(f"/api/v1/objects/{oid}")
    assert resp.status_code == 500
    body = resp.json()
    assert body["error"] == "DATA_CORRUPT"
    assert oid in body["message"]
    assert "0" in body["message"]


def test_read_path_midstream_chunk1_both_corrupt_aborts_stream():
    cluster = ScrubbableFakeCluster()
    payload = (b"A" * 1_048_576) + b"B" * 64
    db_path = "/tmp/test_midstream_abort.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    db.init_db(db_path)
    now = time.time()
    for i in range(1, 5):
        db.upsert_worker(f"worker{i}", f"http://worker{i}:8000", now)

    from worker_client import WorkerClient

    async def scenario():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            # Put object chunks manually
            oid = "midstream-obj"
            c0_data = payload[:1_048_576]
            c1_data = payload[1_048_576:]
            c0_sum = hashlib.sha256(c0_data).hexdigest()
            c1_sum = hashlib.sha256(c1_data).hexdigest()
            db.create_object_pending(oid, "m.bin", len(payload), 2, now)
            db.persist_chunk(f"{oid}_000000", oid, 0, len(c0_data), c0_sum, ["worker1", "worker2"])
            db.persist_chunk(f"{oid}_000001", oid, 1, len(c1_data), c1_sum, ["worker3", "worker4"])
            db.finalize_object(oid, len(payload), hashlib.sha256(payload).hexdigest(), 2, now)
            for w in ("worker1", "worker2"):
                cluster.stores[w][f"{oid}_000000"] = {"data": c0_data, "sidecar": c0_sum}
            for w in ("worker3", "worker4"):
                cluster.stores[w][f"{oid}_000001"] = {"data": b"tampered", "sidecar": c1_sum}

            obj = db.get_object(oid)
            workers = {w["worker_id"]: w for w in db.list_workers()}
            first = await objects_api.fetch_verified_chunk(wc, obj["chunks"][0], workers, object_id=oid)
            gen = objects_api.stream_chunks(
                wc, first, obj["chunks"][1:], workers, obj["checksum"], object_id=oid
            )
            assert await gen.__anext__() == c0_data
            with pytest.raises(objects_api._Corrupt):
                await gen.__anext__()
        finally:
            await wc.close()
            if os.path.exists(db_path):
                os.remove(db_path)

    asyncio.run(scenario())


# --------------------------------------------------- controller scrub & admin
def test_scrub_detects_bitrot_without_reads_and_catches_simultaneous_sidecar_tampering(env):
    client, cluster, app = env
    payload = b"scrub-me-" * 200
    oid = client.post("/api/v1/objects", files={"file": ("s.bin", payload)}).json()["object_id"]
    ch0 = db.get_object(oid)["chunks"][0]
    w1, w2 = [r["worker_id"] for r in ch0["replicas"]]

    # Corrupt w1 data ONLY (local sidecar mismatch caught by worker scrub)
    cluster.stores[w1][ch0["chunk_id"]]["data"] = b"bit-flipped-data"

    # Simultaneously corrupt w2 BOTH data AND sidecar (worker scrub sees ok==True, controller catches vs DB!)
    evil = b"evil-replaced-data"
    cluster.stores[w2][ch0["chunk_id"]] = {
        "data": evil,
        "sidecar": hashlib.sha256(evil).hexdigest(),
    }

    # Mark an unused worker DEAD so it is skipped cleanly while w1 and w2 are both ALIVE
    dead_wid = next(w for w in ("worker1", "worker2", "worker3", "worker4") if w not in (w1, w2))
    db.mark_dead(dead_wid, time.time())

    # Temporarily test scrub_cycle directly (with fresh client) so we inspect both CORRUPT markings
    from worker_client import WorkerClient

    async def run_scrub_only():
        wc = WorkerClient(transport=cluster.transport)
        await wc.start()
        try:
            return await scrubber.scrub_cycle(
                app.state.cfg,
                wc,
                trigger_repair_event=None,
            )
        finally:
            await wc.close()

    res = asyncio.run(run_scrub_only())
    assert res["workers_scrubbed"] == 3  # worker1, worker2, worker3 (worker4 DEAD skipped)
    assert res["chunks_checked"] == 2
    assert res["corrupt_found"] == 2
    assert res["repairs_triggered"] is False

    reps = {r["worker_id"]: r["state"] for r in db.get_chunk_replicas_detail(ch0["chunk_id"])}
    assert reps[w1] == "CORRUPT"
    assert reps[w2] == "CORRUPT"

    # Verify admin replication reports unrecoverable
    rep_admin = client.get("/api/v1/admin/replication").json()
    assert rep_admin["total_chunks"] == 1
    assert rep_admin["unrecoverable"] == 1
    assert rep_admin["scrub"]["chunks_checked"] == 2
    assert rep_admin["scrub"]["corrupt_found"] == 2


def test_admin_scrub_endpoint_summary_and_triggers_repair(env):
    client, cluster, _ = env
    payload = b"heal-via-scrub" * 100
    oid = client.post("/api/v1/objects", files={"file": ("heal.bin", payload)}).json()["object_id"]
    ch0 = db.get_object(oid)["chunks"][0]
    w1, w2 = [r["worker_id"] for r in ch0["replicas"]]

    # Bit-flip w1 ONLY (w2 stays healthy so repair can self-heal)
    cluster.stores[w1][ch0["chunk_id"]]["data"] = b"corrupt-bytes"

    scrub_resp = client.post("/api/v1/admin/scrub")
    assert scrub_resp.status_code == 200
    s_json = scrub_resp.json()
    assert s_json["workers_scrubbed"] == 4
    assert s_json["chunks_checked"] == 2
    assert s_json["corrupt_found"] == 1
    assert s_json["repairs_triggered"] is True

    # Repair loop wakes immediately on trigger_repair_event and replaces corrupted replica
    for _ in range(60):
        info = client.get("/api/v1/admin/replication").json()
        if info["healthy"] == 1 and info["under_replicated"] == 0:
            break
        time.sleep(0.02)

    info = client.get("/api/v1/admin/replication").json()
    assert info["total_chunks"] == 1
    assert info["healthy"] == 1
    assert info["under_replicated"] == 0
    assert info["scrub"]["chunks_checked"] == 2
    assert info["scrub"]["corrupt_found"] == 1
    assert info["scrub"]["last_scrub_at"] is not None

    chunks_resp = client.get(f"/api/v1/admin/objects/{oid}/chunks")
    assert chunks_resp.status_code == 200
    c_body = chunks_resp.json()
    assert len(c_body["chunks"]) == 1
    assert c_body["chunks"][0]["index"] == 0
    assert c_body["chunks"][0]["chunk_id"] == ch0["chunk_id"]
    reps = c_body["chunks"][0]["replicas"]
    assert len(reps) == 2
    assert all(r["state"] == "OK" and r["worker_status"] == "ALIVE" for r in reps)
    assert w1 not in {r["worker_id"] for r in reps}

    # Missing object -> 404
    missing = client.get("/api/v1/admin/objects/nonexistent/chunks")
    assert missing.status_code == 404
    assert missing.json()["error"] == "OBJECT_NOT_FOUND"
