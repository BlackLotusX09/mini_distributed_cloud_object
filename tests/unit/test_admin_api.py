import os
import sys
import time
from contextlib import ExitStack

import httpx
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "controller"))

import admin_api  # noqa: E402
import db  # noqa: E402
import main  # noqa: E402
import settings as settings_mod  # noqa: E402


class _MemoryWorkerTransport(httpx.AsyncBaseTransport):
    """Minimal in-memory worker transport supporting chunk PUT/GET/DELETE & scrub."""

    def __init__(self, fail_puts_for=None, corrupt_reads_for=None):
        self.store = {}
        self.fail_puts_for = set(fail_puts_for or ())
        self.corrupt_reads_for = set(corrupt_reads_for or ())

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        path = request.url.path
        if path.startswith("/internal/chunks/"):
            cid = path[len("/internal/chunks/"):]
            if request.method == "PUT":
                if wid in self.fail_puts_for:
                    return httpx.Response(500, json={"error": "BOOM", "message": "injected failure"})
                self.store[(wid, cid)] = request.content
                return httpx.Response(201, json={"chunk_id": cid, "size": len(request.content)})
            if request.method == "GET":
                if (wid, cid) in self.corrupt_reads_for:
                    return httpx.Response(409, json={"error": "CHUNK_CORRUPT", "message": "bad checksum"})
                data = self.store.get((wid, cid))
                if data is None:
                    return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND", "message": "missing"})
                return httpx.Response(200, content=data)
            if request.method == "DELETE":
                self.store.pop((wid, cid), None)
                return httpx.Response(204)
        if path == "/internal/scrub" and request.method == "POST":
            cids = [c for (w, c) in self.store if w == wid]
            return httpx.Response(200, json={
                "worker_id": wid,
                "checked": len(cids),
                "ok": len(cids),
                "corrupt": [],
                "missing_checksum": [],
                "checksums": {},
            })
        return httpx.Response(404, json={"error": "NOT_FOUND", "message": path})


@pytest.fixture
def make_client(tmp_path):
    stack = ExitStack()
    path = str(tmp_path / "controller.db")

    def factory(transport=None, **kw):
        cfg = settings_mod.Settings(
            db_path=path,
            heartbeat_timeout=kw.pop("heartbeat_timeout", 15.0),
            liveness_check_interval=kw.pop("liveness_check_interval", 60.0),
            repair_interval=kw.pop("repair_interval", 60.0),
            scrub_interval=kw.pop("scrub_interval", 300.0),
            **kw,
        )
        return stack.enter_context(TestClient(main.create_app(cfg, worker_transport=transport)))

    factory.db_path = path
    yield factory
    stack.close()


def _register_all(client, count=4):
    for i in range(1, count + 1):
        wid = f"worker{i}"
        r = client.post("/internal/workers/register", json={"worker_id": wid, "address": f"http://{wid}:8000"})
        assert r.status_code == 200


def test_events_logged_on_transitions_not_routine_heartbeats(make_client):
    transport = _MemoryWorkerTransport()
    client = make_client(transport=transport)
    _register_all(client, 3)

    ev_after_reg = client.get("/api/v1/admin/events").json()
    assert len(ev_after_reg["events"]) == 3
    assert [e["type"] for e in ev_after_reg["events"]] == ["worker_registered"] * 3
    since_id = ev_after_reg["latest_id"]

    # Routine ALIVE heartbeats MUST produce zero events
    for _ in range(5):
        r = client.post("/internal/workers/heartbeat", json={"worker_id": "worker1"})
        assert r.status_code == 200
    ev_none = client.get("/api/v1/admin/events", params={"since": since_id}).json()
    assert ev_none["events"] == []
    assert ev_none["latest_id"] == since_id

    # DEAD transition and heartbeat recovery must each emit one event
    assert db.mark_dead("worker2", time.time()) is True
    assert client.post("/internal/workers/heartbeat", json={"worker_id": "worker2"}).status_code == 200

    # Upload commit, delete, and scrub must emit events
    up = client.post("/api/v1/objects", files={"file": ("hello.bin", b"A" * 2048, "application/octet-stream")})
    assert up.status_code == 201
    oid = up.json()["object_id"]

    scrub = client.post("/api/v1/admin/scrub")
    assert scrub.status_code == 200

    dele = client.delete(f"/api/v1/objects/{oid}")
    assert dele.status_code == 204

    ev_later = client.get("/api/v1/admin/events", params={"since": since_id}).json()
    types = [e["type"] for e in ev_later["events"]]
    assert types == ["worker_dead", "worker_alive", "object_committed", "scrub_finished", "object_deleted"]
    assert ev_later["events"][0]["severity"] == "ERROR"
    assert ev_later["events"][2]["data"]["size"] == 2048
    assert ev_later["latest_id"] > since_id


def test_events_pruning_keeps_latest_1000_and_since_limit_work(make_client):
    client = make_client(transport=_MemoryWorkerTransport())
    for i in range(1035):
        admin_api.record_event("worker_registered", "INFO", f"event-{i}", seq=i)

    with db.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert count == admin_api.MAX_EVENTS_RETAINED

    page1 = client.get("/api/v1/admin/events", params={"since": 0, "limit": 10}).json()
    assert len(page1["events"]) == 10
    assert [e["data"]["seq"] for e in page1["events"]] == list(range(35, 45))

    page2 = client.get("/api/v1/admin/events", params={"since": page1["events"][-1]["id"], "limit": 5}).json()
    assert len(page2["events"]) == 5
    assert [e["data"]["seq"] for e in page2["events"]] == list(range(45, 50))
    assert page2["latest_id"] == 1035


def test_rollback_and_corrupt_events(make_client):
    # Only 2 workers alive, and worker2 fails PUTs -> triggers object_rolled_back
    client_fail = make_client(transport=_MemoryWorkerTransport(fail_puts_for={"worker2"}))
    _register_all(client_fail, 2)
    since = client_fail.get("/api/v1/admin/events").json()["latest_id"]

    up_fail = client_fail.post("/api/v1/objects", files={"file": ("bad.bin", b"Z" * 100, "application/octet-stream")})
    assert up_fail.status_code == 503
    evs = client_fail.get("/api/v1/admin/events", params={"since": since}).json()["events"]
    assert any(e["type"] == "object_rolled_back" and e["severity"] == "WARN" for e in evs)


def test_corrupt_replica_read_emits_event(make_client, tmp_path):
    transport = _MemoryWorkerTransport()
    client = make_client(transport=transport)
    _register_all(client, 3)

    up = client.post("/api/v1/objects", files={"file": ("good.bin", b"X" * 512, "application/octet-stream")})
    assert up.status_code == 201
    oid = up.json()["object_id"]
    chunks = client.get(f"/api/v1/admin/objects/{oid}/chunks").json()["chunks"]
    cid = chunks[0]["chunk_id"]
    victim = chunks[0]["replicas"][0]["worker_id"]
    healthy = chunks[0]["replicas"][1]["worker_id"]
    transport.corrupt_reads_for.add((victim, cid))
    # Mark surviving replica DEAD so fetch_verified_chunk deterministically tries ALIVE victim first
    assert db.mark_dead(healthy, time.time()) is True

    since = client.get("/api/v1/admin/events").json()["latest_id"]
    dl = client.get(f"/api/v1/objects/{oid}")
    assert dl.status_code == 200
    assert dl.content == b"X" * 512

    evs = client.get("/api/v1/admin/events", params={"since": since}).json()["events"]
    assert any(e["type"] == "replica_corrupt" and e["severity"] == "ERROR" for e in evs)


def test_admin_overview_aggregates_totals_used_bytes_and_config(make_client):
    transport = _MemoryWorkerTransport()
    client = make_client(transport=transport)
    _register_all(client, 4)

    up = client.post("/api/v1/objects", files={"file": ("data.bin", b"Q" * 10_000, "application/octet-stream")})
    assert up.status_code == 201

    ov = client.get("/api/v1/admin/overview")
    assert ov.status_code == 200
    body = ov.json()

    assert body["totals"]["objects"] == 1
    assert body["totals"]["chunks"] == 1
    assert body["totals"]["logical_bytes"] == 10_000
    assert body["totals"]["raw_bytes"] == 20_000
    assert body["replication"]["total_chunks"] == 1
    assert body["replication"]["healthy"] == 1
    assert body["replication"]["corrupt"] == 0
    assert sum(w["used_bytes"] for w in body["workers"]) == 20_000
    assert body["config"]["replication_factor"] == 2
    assert body["config"]["chunk_size"] == 1_048_576
    assert body["config"]["heartbeat_timeout"] == 15.0
    assert body["config"]["repair_grace_period"] == 30.0


def test_admin_enabled_false_blocks_all_admin_routes_with_403(make_client, monkeypatch):
    client = make_client(transport=_MemoryWorkerTransport())
    _register_all(client, 2)

    monkeypatch.setenv("ADMIN_ENABLED", "false")
    for path in (
        "/api/v1/admin/overview",
        "/api/v1/admin/events",
        "/api/v1/admin/workers",
        "/api/v1/admin/replication",
        "/api/v1/admin/placement-test",
    ):
        resp = client.get(path)
        assert resp.status_code == 403
        assert resp.json()["error"] == "ADMIN_DISABLED"

    scrub_resp = client.post("/api/v1/admin/scrub")
    assert scrub_resp.status_code == 403
    assert scrub_resp.json()["error"] == "ADMIN_DISABLED"

    # Public client API remains unaffected
    up = client.post("/api/v1/objects", files={"file": ("ok.txt", b"hello", "text/plain")})
    assert up.status_code == 201


def test_ui_spa_fallback_and_asset_404(make_client, tmp_path, monkeypatch):
    dist = tmp_path / "dist"
    assets = dist / "assets"
    assets.mkdir(parents=True)
    (dist / "index.html").write_text("<html><body>DFS UI</body></html>", encoding="utf-8")
    (assets / "app.js").write_text("console.log('ok');", encoding="utf-8")

    monkeypatch.setenv("FRONTEND_DIST", str(dist))
    client = make_client(transport=_MemoryWorkerTransport())

    for route in ("/ui", "/ui/", "/ui/objects", "/ui/events", "/ui/objects/some-uuid"):
        r = client.get(route)
        assert r.status_code == 200
        assert "DFS UI" in r.text

    js = client.get("/ui/assets/app.js")
    assert js.status_code == 200
    assert "console.log" in js.text

    missing_js = client.get("/ui/assets/missing.js")
    assert missing_js.status_code == 404


def test_repair_events_and_cors_headers(make_client, monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", "http://localhost:5173")
    transport = _MemoryWorkerTransport()
    client = make_client(transport=transport)
    _register_all(client, 3)

    up = client.post("/api/v1/objects", files={"file": ("rep.bin", b"R" * 1024, "application/octet-stream")})
    assert up.status_code == 201
    oid = up.json()["object_id"]
    chunks = client.get(f"/api/v1/admin/objects/{oid}/chunks").json()["chunks"]
    cid = chunks[0]["chunk_id"]
    dead_w = chunks[0]["replicas"][0]["worker_id"]
    assert db.mark_dead(dead_w, time.time() - 60.0) is True

    since = client.get("/api/v1/admin/events").json()["latest_id"]
    import asyncio
    import repair as repair_mod

    res = asyncio.run(repair_mod.copy_chunk(cid, client.app.state.worker_client, replication_factor=2))
    assert res[0] == "repaired"

    evs = client.get("/api/v1/admin/events", params={"since": since}).json()["events"]
    assert [e["type"] for e in evs] == ["repair_started", "repair_completed"]

    # CORS preflight check
    cors_resp = client.options(
        "/api/v1/admin/overview",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"},
    )
    assert cors_resp.headers.get("access-control-allow-origin") == "http://localhost:5173"

