"""Concurrency, scale and restart matrix."""
import asyncio
import hashlib
import json
import os
import tempfile
import threading
import time

import httpx
import pytest

from backends import BASE_URL, WORKERS
from helpers import expect, random_bytes, sha256_hex, wait_for, workers_state

pytestmark = pytest.mark.integration

STRESS_MB = int(os.environ.get("DFS_STRESS_MB", "250"))
MEM_LIMIT_KB = int(os.environ.get("DFS_CONTROLLER_RSS_LIMIT_MB", "400")) * 1024


@pytest.mark.slow
def test_large_stream_exact_hash_and_bounded_controller_memory(cluster, client):
    total = STRESS_MB * 1024 * 1024
    src = tempfile.NamedTemporaryFile(delete=False)
    h = hashlib.sha256()
    try:
        for _ in range(STRESS_MB):
            blk = os.urandom(1024 * 1024)
            h.update(blk)
            src.write(blk)
        src.close()
        expected = h.hexdigest()

        peak = [0]
        stop = threading.Event()

        def sampler():
            while not stop.is_set():
                rss = cluster.controller_rss_kb()
                if rss:
                    peak[0] = max(peak[0], rss)
                time.sleep(0.5)

        t = threading.Thread(target=sampler, daemon=True)
        t.start()
        oid = None
        try:
            with open(src.name, "rb") as f:
                r = client.post("/api/v1/objects", files={"file": ("big.bin", f, "application/octet-stream")},
                                timeout=1800)
            expect(r, 201)
            oid = r.json()["object_id"]
            assert r.json()["size"] == total and r.json()["checksum"] == expected
            dh, got = hashlib.sha256(), 0
            with client.stream("GET", f"/api/v1/objects/{oid}", timeout=1800) as resp:
                assert resp.status_code == 200, resp.read()
                for blk in resp.iter_bytes(1024 * 1024):
                    dh.update(blk)
                    got += len(blk)
            assert got == total and dh.hexdigest() == expected
        finally:
            stop.set()
            t.join(timeout=5)
            if oid:
                client.delete(f"/api/v1/objects/{oid}")
        if peak[0]:
            assert peak[0] < MEM_LIMIT_KB, f"controller RSS peaked at {peak[0] // 1024} MB (limit {MEM_LIMIT_KB // 1024} MB)"
    finally:
        os.unlink(src.name)


@pytest.mark.slow
def test_parallel_uploads_and_downloads(cluster, client, uploaded_object):
    seeds = [uploaded_object(2 * 1024 * 1024 + i * 777) for i in range(10)]
    uploads = [random_bytes(1_500_000 + i * 1111) for i in range(10)]

    async def run():
        limits = httpx.Limits(max_connections=40)
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=300, limits=limits) as ac:
            async def up(data):
                return await ac.post("/api/v1/objects", files={"file": ("p.bin", data, "application/octet-stream")})

            async def down(oid):
                return await ac.get(f"/api/v1/objects/{oid}")

            res = await asyncio.gather(*[up(d) for d in uploads], *[down(s[0]) for s in seeds])
            return res[:10], res[10:]

    ups, downs = asyncio.run(run())
    created = []
    try:
        for r, data in zip(ups, uploads):
            assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
            created.append(r.json()["object_id"])
            assert r.json()["checksum"] == sha256_hex(data)
        for r, (oid, raw, digest) in zip(downs, seeds):
            assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"
            assert sha256_hex(r.content) == digest
        for oid, data in zip(created, uploads):
            assert sha256_hex(client.get(f"/api/v1/objects/{oid}").content) == sha256_hex(data)
    finally:
        for oid in created:
            client.delete(f"/api/v1/objects/{oid}")


def test_concurrent_metadata_patches_lose_nothing(client, uploaded_object):
    oid, _, _ = uploaded_object(1000, metadata={"base": "0"})

    async def run():
        async with httpx.AsyncClient(base_url=BASE_URL, timeout=60) as ac:
            return await asyncio.gather(*[
                ac.patch(f"/api/v1/objects/{oid}/metadata", json={f"key{i}": f"v{i}"}) for i in range(10)])

    for r in asyncio.run(run()):
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.text}"
    custom = client.get(f"/api/v1/objects/{oid}/metadata").json()["custom"]
    assert custom == {"base": "0", **{f"key{i}": f"v{i}" for i in range(10)}}, custom


# ------------------------------------------------------------ restart matrix
def _wait_ready(client):
    def ok():
        if client.get("/health").status_code != 200:
            return False
        ws = workers_state(client)
        return len(ws) >= 4 and all(w["status"] == "ALIVE" for w in ws.values())
    wait_for(ok, timeout=180, interval=1.0, desc="controller healthy and 4 workers ALIVE")


@pytest.mark.slow
def test_restart_matrix_preserves_every_object(cluster, client, uploaded_object):
    objs = [uploaded_object(n, metadata={"n": str(n)}) for n in (0, 4096, 3 * 1024 * 1024 + 5)]

    def verify(stage):
        for oid, raw, digest in objs:
            r = client.get(f"/api/v1/objects/{oid}")
            assert r.status_code == 200, f"[{stage}] Expected 200, got {r.status_code}: {r.text}"
            assert sha256_hex(r.content) == digest, f"[{stage}] sha mismatch for {oid}"
            m = client.get(f"/api/v1/objects/{oid}/metadata")
            assert m.status_code == 200 and m.json()["custom"], f"[{stage}] metadata lost" if raw else True

    stages = [
        ("controller", cluster.restart_controller),
        ("worker1", lambda: cluster.restart_workers(["worker1"])),
        ("all workers", lambda: cluster.restart_workers(WORKERS)),
        ("full down/up", cluster.full_restart),
    ]
    for name, action in stages:
        action()
        if name == "full down/up":
            wait_for(lambda: client.get("/health").status_code == 200, timeout=180, desc="controller back")
            cluster.after_up()
        _wait_ready(client)
        verify(name)


def test_cluster_invariants_script_passes(cluster, client, uploaded_object):
    import subprocess
    import sys

    uploaded_object(2 * 1024 * 1024, metadata={"check": "invariants"})
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    cmd = [sys.executable, os.path.join(root, "scripts", "check_invariants.py")]
    if cluster.name == "local":
        cmd.extend(["--db", cluster.db_path, "--data-root", cluster.tmp])
    # Wait briefly for any asynchronous cleanup/reconciliation before checking strict invariants
    wait_for(
        lambda: subprocess.run(cmd, capture_output=True, text=True).returncode == 0,
        timeout=30,
        interval=0.5,
        desc="scripts/check_invariants.py exit 0",
    )
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"{res.stdout}\n{res.stderr}"
    assert "OK: all cluster invariants hold" in res.stdout
