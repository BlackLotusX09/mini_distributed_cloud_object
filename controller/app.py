"""Controller skeleton (Day 1): schema init, /health, worker register + heartbeat.

Standard library only, so the image needs no pip install.
"""
import json
import os
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8000"))
DB_PATH = os.environ.get("DB_PATH", "/data/metadata/metadata.db")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")
HEARTBEAT_TIMEOUT_SEC = int(os.environ.get("HEARTBEAT_TIMEOUT_SEC", "10"))

_db_lock = threading.Lock()


def connect(db_path=DB_PATH):
    d = os.path.dirname(db_path)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(conn):
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    conn.commit()


class Handler(BaseHTTPRequestHandler):
    conn = None  # set in main()

    def _json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            with _db_lock:
                self.conn.execute("SELECT 1").fetchone()
                alive = self.conn.execute(
                    "SELECT COUNT(*) FROM workers WHERE status='ACTIVE' "
                    "AND last_heartbeat >= datetime('now', ?)",
                    (f"-{HEARTBEAT_TIMEOUT_SEC} seconds",),
                ).fetchone()[0]
            return self._json(200, {"status": "ok", "role": "controller", "active_workers": alive})
        if self.path == "/internal/workers":
            with _db_lock:
                rows = self.conn.execute("SELECT * FROM workers ORDER BY id").fetchall()
            return self._json(200, [dict(r) for r in rows])
        self._json(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = self._read_json()
        except ValueError:
            return self._json(400, {"error": "invalid json"})
        if self.path in ("/internal/workers/register", "/internal/workers/heartbeat"):
            wid, addr = body.get("id"), body.get("address")
            if not wid or not addr:
                return self._json(400, {"error": "id and address required"})
            with _db_lock:
                self.conn.execute(
                    "INSERT INTO workers (id, address, status, last_heartbeat) "
                    "VALUES (?, ?, 'ACTIVE', CURRENT_TIMESTAMP) "
                    "ON CONFLICT(id) DO UPDATE SET address=excluded.address, "
                    "status='ACTIVE', last_heartbeat=CURRENT_TIMESTAMP",
                    (wid, addr),
                )
                self.conn.commit()
            return self._json(200, {"ok": True})
        self._json(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        print("[controller]", fmt % args, flush=True)


def main():
    conn = connect()
    init_schema(conn)
    Handler.conn = conn
    print(f"[controller] listening on :{PORT}, db={DB_PATH}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
