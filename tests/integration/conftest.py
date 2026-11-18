import contextlib
import json
import os
import sys

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backends import BASE_URL, WORKERS, choose_backend  # noqa: E402
from helpers import (expect, get_chunk_map, random_bytes, sha256_hex, wait_for, wait_worker,  # noqa: E401, E402
                     workers_state)


def pytest_collection_modifyitems(items):
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


def _ready(client):
    if client.get("/health").status_code != 200:
        return False
    ws = workers_state(client)
    return len(ws) >= 4 and all(w["status"] == "ALIVE" for w in ws.values())


@pytest.fixture(scope="session")
def cluster():
    """Verify/start the cluster and wait until /health is 200 and all 4 workers are ALIVE."""
    be = choose_backend()
    probe = httpx.Client(base_url=BASE_URL, timeout=5)
    try:
        already = probe.get("/health").status_code == 200
    except httpx.HTTPError:
        already = False
    started = False
    if not already:
        be.up()
        started = True
        wait_for(lambda: probe.get("/health").status_code == 200, timeout=180, desc="controller /health")
        be.after_up()
    wait_for(lambda: _ready(probe), timeout=120, desc="4 workers ALIVE")
    be.started_here = started
    yield be
    probe.close()
    if started:
        be.cleanup()


@pytest.fixture(scope="session")
def client(cluster):
    with httpx.Client(base_url=BASE_URL, timeout=httpx.Timeout(120.0, connect=10.0)) as c:
        yield c


@pytest.fixture
def uploaded_object(client):
    """Factory: uploaded_object(size, metadata=None, name=..., content_type=...) ->
    (object_id, raw_bytes, sha256_hex). Every created object is DELETEd on teardown."""
    created = []

    def make(size=1024, metadata=None, name="blob.bin", content_type="application/octet-stream", data=None):
        raw = data if data is not None else random_bytes(size)
        form = {"metadata": json.dumps(metadata)} if metadata is not None else {}
        r = client.post("/api/v1/objects", files={"file": (name, raw, content_type)}, data=form)
        expect(r, 201)
        oid = r.json()["object_id"]
        created.append(oid)
        return oid, raw, sha256_hex(raw)

    yield make
    for oid in created:
        with contextlib.suppress(httpx.HTTPError):
            client.delete(f"/api/v1/objects/{oid}")


@pytest.fixture
def stop_worker(cluster, client):
    """Context-manager factory. Stops the worker, waits for DEAD, yields; the finally block always
    restarts it and waits for ALIVE so one failing test cannot poison the next."""

    @contextlib.contextmanager
    def ctx(name):
        cluster.stop_worker(name)
        try:
            wait_worker(client, name, "DEAD", timeout=90)
            yield
        finally:
            cluster.start_worker_and_fix(name)
            wait_worker(client, name, "ALIVE", timeout=90)

    return ctx


@pytest.fixture
def pause_worker(cluster, client):
    @contextlib.contextmanager
    def ctx(name):
        cluster.pause_worker(name)
        try:
            yield
        finally:
            cluster.unpause_worker(name)
            wait_worker(client, name, "ALIVE", timeout=90)

    return ctx


@pytest.fixture
def corrupt_chunk_on_disk(cluster):
    def do(worker, chunk_id):
        cluster.corrupt_chunk(worker, chunk_id)  # flips bytes in the payload; sidecar untouched
    return do
