"""Unit tests for client/objstore.py CLI subcommands and error handling."""
import hashlib
import json
import os
import subprocess
import sys
import threading

import pytest
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "client"))

import objstore  # noqa: E402


def _make_fake_server(tmp_path):
    """Lightweight WSGI/HTTP controller server backed by FastAPI TestClient over stdlib HTTPServer."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import httpx
    import main
    import settings as settings_mod

    stores = {f"worker{i}": {} for i in range(1, 5)}

    def handler_transport(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        path = request.url.path
        store = stores[host]
        if path == "/internal/scrub" and request.method == "POST":
            sums = {cid: hashlib.sha256(d).hexdigest() for cid, d in store.items()}
            return httpx.Response(200, json={
                "checked": len(store), "ok": len(store),
                "checksums": sums, "corrupt": [], "missing_checksum": [],
            })
        if path.startswith("/internal/chunks/"):
            cid = path.rsplit("/", 1)[-1]
            if request.method == "PUT":
                store[cid] = bytes(request.content)
                return httpx.Response(201, json={"chunk_id": cid})
            if request.method == "GET":
                if cid not in store:
                    return httpx.Response(404, json={"error": "CHUNK_NOT_FOUND"})
                return httpx.Response(200, content=store[cid])
            if request.method == "DELETE":
                store.pop(cid, None)
                return httpx.Response(204)
        return httpx.Response(404)

    cfg = settings_mod.Settings(
        db_path=str(tmp_path / "cli.db"),
        liveness_check_interval=60.0,
        repair_interval=60.0,
        scrub_interval=600.0,
    )
    app = main.create_app(cfg, worker_transport=httpx.MockTransport(handler_transport))
    tc = TestClient(app)
    tc.__enter__()
    for i in range(1, 5):
        tc.post("/internal/workers/register",
                json={"worker_id": f"worker{i}", "address": f"http://worker{i}:8000"})

    corrupt_download_bytes = {"enabled": False}

    class Bridge(BaseHTTPRequestHandler):
        def _proxy(self, method):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            hdrs = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
            resp = tc.request(method, self.path, content=body, headers=hdrs)
            content = resp.content
            if corrupt_download_bytes["enabled"] and method == "GET" and "/metadata" not in self.path and resp.status_code == 200:
                content = b"tampered-in-transit"
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() in ("content-length", "transfer-encoding", "connection"):
                    continue
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            if content:
                self.wfile.write(content)

        def do_GET(self):
            self._proxy("GET")

        def do_POST(self):
            self._proxy("POST")

        def do_PATCH(self):
            self._proxy("PATCH")

        def do_DELETE(self):
            self._proxy("DELETE")

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Bridge)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, tc, corrupt_download_bytes


def _run_cli(args):
    return subprocess.run(
        [sys.executable, os.path.join(ROOT, "client", "objstore.py"), *args],
        capture_output=True, text=True,
    )


def test_cli_all_subcommands_and_error_paths(tmp_path):
    srv, tc, corrupt_flag = _make_fake_server(tmp_path)
    base_url = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        src = tmp_path / "hello.txt"
        payload = b"hello distributed CLI! " * 100
        src.write_bytes(payload)
        expected_sha = hashlib.sha256(payload).hexdigest()

        # 1. upload (--json)
        r = _run_cli(["--url", base_url, "upload", str(src), "--meta", "env=dev", "--meta", "owner=alice", "--json"])
        assert r.returncode == 0, r.stderr
        up = json.loads(r.stdout)
        oid = up["object_id"]
        assert up["checksum"] == expected_sha
        assert up["metadata"] == {"env": "dev", "owner": "alice"}

        # 2. info (table + --json)
        r_info = _run_cli(["info", oid, "--url", base_url])
        assert r_info.returncode == 0, r_info.stderr
        assert "hello.txt" in r_info.stdout and "alice" in r_info.stdout

        # 3. patch (update + delete via empty/null)
        r_patch = _run_cli(["--url", base_url, "patch", oid, "env=prod", "tier=gold", "owner=null", "--json"])
        assert r_patch.returncode == 0, r_patch.stderr
        patched = json.loads(r_patch.stdout)["custom"]
        assert patched == {"env": "prod", "tier": "gold"}

        # 4. list (table pagination footer + --json)
        r_list = _run_cli(["--url", base_url, "list", "--limit", "10", "--offset", "0"])
        assert r_list.returncode == 0, r_list.stderr
        assert oid in r_list.stdout and "Showing 1–1 of 1" in r_list.stdout

        # 5. download + SHA-256 verification
        dst = tmp_path / "downloaded.txt"
        r_dl = _run_cli(["--url", base_url, "download", oid, "-o", str(dst)])
        assert r_dl.returncode == 0, r_dl.stderr
        assert f"Checksum: {expected_sha} (VERIFIED)" in r_dl.stdout
        assert dst.read_bytes() == payload

        # 6. download checksum mismatch exits non-zero
        bad_dst = tmp_path / "bad.txt"
        corrupt_flag["enabled"] = True
        r_bad_dl = _run_cli(["--url", base_url, "download", oid, "-o", str(bad_dst)])
        corrupt_flag["enabled"] = False
        assert r_bad_dl.returncode == 1
        assert "MISMATCH" in r_bad_dl.stderr
        assert not bad_dst.exists()

        # 7. admin: workers, chunks, replication, scrub
        r_w = _run_cli(["--url", base_url, "workers"])
        assert r_w.returncode == 0 and "worker1" in r_w.stdout and "ALIVE" in r_w.stdout

        r_ch = _run_cli(["--url", base_url, "chunks", oid])
        assert r_ch.returncode == 0 and f"{oid}_000000" in r_ch.stdout

        r_rep = _run_cli(["--url", base_url, "replication", "--json"])
        assert r_rep.returncode == 0 and json.loads(r_rep.stdout)["healthy"] == 1

        r_scr = _run_cli(["--url", base_url, "scrub", "--json"])
        assert r_scr.returncode == 0 and json.loads(r_scr.stdout)["workers_scrubbed"] == 4

        # 8. delete + 404 after delete
        r_del = _run_cli(["--url", base_url, "delete", oid])
        assert r_del.returncode == 0 and f"Deleted object {oid}" in r_del.stdout

        r_404 = _run_cli(["--url", base_url, "info", oid])
        assert r_404.returncode != 0
        assert "404" in r_404.stderr or "OBJECT_NOT_FOUND" in r_404.stderr

        # 9. unreachable controller exits non-zero cleanly
        r_conn = _run_cli(["--url", "http://127.0.0.1:9", "workers"])
        assert r_conn.returncode == 2
        assert "connection error" in r_conn.stderr
    finally:
        srv.shutdown()
        srv.server_close()
        tc.__exit__(None, None, None)
