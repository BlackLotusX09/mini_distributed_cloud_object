"""Worker skeleton (Day 1): /health, register + 3s heartbeat to the controller.

Chunk PUT/GET/DELETE endpoints arrive on Day 2. Standard library only.
"""
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WORKER_ID = os.environ.get("WORKER_ID", "worker1")
PORT = int(os.environ.get("PORT", "8001"))
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
DATA_DIR = os.environ.get("DATA_DIR", "/data/chunks")
HEARTBEAT_SEC = float(os.environ.get("HEARTBEAT_SEC", "3"))
ADDRESS = os.environ.get("WORKER_ADDRESS", f"http://{WORKER_ID}:{PORT}")


def post(path, payload):
    req = urllib.request.Request(
        CONTROLLER_URL + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=3) as r:
        return r.status


def heartbeat_loop():
    """Register (first successful call) then heartbeat forever; retry on failure."""
    path = "/internal/workers/register"
    while True:
        try:
            post(path, {"id": WORKER_ID, "address": ADDRESS})
            path = "/internal/workers/heartbeat"
        except Exception as e:  # controller not up yet / network blip
            print(f"[{WORKER_ID}] controller unreachable: {e}", flush=True)
            path = "/internal/workers/register"
        time.sleep(HEARTBEAT_SEC)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            try:
                n = len(os.listdir(DATA_DIR))
            except OSError:
                n = -1
            data = json.dumps(
                {"status": "ok", "role": "worker", "id": WORKER_ID, "chunks": n}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        print(f"[{WORKER_ID}]", fmt % args, flush=True)


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    print(f"[{WORKER_ID}] listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
