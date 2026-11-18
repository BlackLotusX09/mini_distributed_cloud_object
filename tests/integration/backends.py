"""Cluster backends for integration tests.

DockerBackend drives the real Docker Compose stack. LocalBackend runs the same code as local
processes (controller :8000, worker1..4 :8001-8004) and is used automatically when Docker is
unavailable, with identical semantics for stop/pause/restart/corrupt.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import httpx

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import check_invariants as ci  # noqa: E402

BASE_URL = os.environ.get("DFS_BASE_URL", "http://localhost:8000")
WORKERS = ["worker1", "worker2", "worker3", "worker4"]


def sh(*cmd, check=True, timeout=300, cwd=None):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed ({r.returncode}): {r.stderr.strip() or r.stdout.strip()}")
    return r


def _flip_byte(data: bytes) -> bytes:
    b = bytearray(data)
    b[len(b) // 2] ^= 0xFF
    return bytes(b)


class LocalBackend:
    name = "local"
    # shortened timings so liveness/repair tests finish quickly; Docker uses compose defaults
    env = {"HEARTBEAT_TIMEOUT": "6", "LIVENESS_CHECK_INTERVAL": "1", "REPAIR_GRACE_PERIOD": "8",
           "REPAIR_INTERVAL": "2", "SCRUB_INTERVAL": "3600", "WORKER_READ_TIMEOUT": "3"}
    repair_grace = 8.0
    read_timeout = 3.0

    def __init__(self):
        self.tmp = tempfile.mkdtemp(prefix="dfs_local_")
        self.db_path = os.path.join(self.tmp, "controller.db")
        self.procs = {}
        self.log_offsets = {}
        self.src = ci.LocalSource(self.db_path, self.tmp)
        self.py = os.path.join(ROOT, ".venv", "bin", "python")
        if not os.path.exists(self.py):
            self.py = sys.executable

    # ---- lifecycle
    def _spawn(self, name, cmd, env, cwd):
        log_path = os.path.join(self.tmp, f"{name}.log")
        self.log_offsets[name] = os.path.getsize(log_path) if os.path.exists(log_path) else 0
        with open(log_path, "ab") as log:
            self.procs[name] = subprocess.Popen(cmd, cwd=cwd, env={**os.environ, **env},
                                                stdout=log, stderr=subprocess.STDOUT)

    def start_controller(self):
        self._spawn("controller", [self.py, "-m", "uvicorn", "--app-dir", "controller", "main:app",
                                   "--port", "8000", "--log-level", "warning"],
                    {"DB_PATH": self.db_path, **self.env}, ROOT)

    def start_worker(self, name):
        n = name[-1]
        self._spawn(name, [self.py, "app.py"],
                    {"WORKER_ID": name, "PORT": f"800{n}", "DATA_DIR": os.path.join(self.tmp, name),
                     "CONTROLLER_URL": "http://127.0.0.1:8000", "HEARTBEAT_INTERVAL": "1s",
                     "WORKER_SCRUB_YIELD_DELAY": "0"},
                    os.path.join(ROOT, "worker"))

    def fix_address(self, name):
        """Wait until the spawned worker has registered itself, then point its address at 127.0.0.1."""
        n = name[-1]
        log_path = os.path.join(self.tmp, f"{name}.log")
        offset = self.log_offsets.get(name, 0)
        deadline = time.time() + 30
        registered = False
        while time.time() < deadline:
            if not registered and os.path.exists(log_path):
                with open(log_path, "rb") as f:
                    f.seek(offset)
                    if b"registered as" in f.read():
                        registered = True
            if registered:
                try:
                    if httpx.get(f"http://127.0.0.1:800{n}/health", timeout=2).status_code == 200:
                        r = httpx.post(
                            f"{BASE_URL}/internal/workers/register",
                            timeout=3,
                            json={"worker_id": name, "address": f"http://127.0.0.1:800{n}"},
                        )
                        if r.status_code == 200:
                            return
                except httpx.HTTPError:
                    pass
            time.sleep(0.1)
        raise TimeoutError(f"fix_address timed out waiting for {name} to register and listen")

    def up(self):
        self.start_controller()
        for w in WORKERS:
            self.start_worker(w)

    def after_up(self):
        for w in WORKERS:
            self.fix_address(w)

    def down(self):
        for name in list(self.procs):
            self._kill(name)

    def cleanup(self):
        self.down()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _kill(self, name, sig=signal.SIGTERM):
        p = self.procs.pop(name, None)
        if p and p.poll() is None:
            p.send_signal(signal.SIGCONT)
            p.send_signal(sig)
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    # ---- fault injection
    def stop_worker(self, name):
        self._kill(name)

    def start_worker_and_fix(self, name):
        self.start_worker(name)
        self.fix_address(name)

    def pause_worker(self, name):
        self.procs[name].send_signal(signal.SIGSTOP)

    def unpause_worker(self, name):
        self.procs[name].send_signal(signal.SIGCONT)

    def restart_controller(self):
        self._kill("controller")
        self.start_controller()

    def restart_workers(self, names):
        for n in names:
            self._kill(n)
        for n in names:
            self.start_worker(n)
        for n in names:
            self.fix_address(n)

    def full_restart(self):
        self.down()  # volumes (data dirs + sqlite) are preserved
        self.up()

    # ---- disk / db access
    def worker_files(self, worker):
        return self.src.worker_files(worker)

    def chunk_path(self, worker, cid):
        return os.path.join(self.tmp, worker, "chunks", cid[:2], cid + ".chunk")

    def corrupt_chunk(self, worker, cid):
        p = self.chunk_path(worker, cid)
        with open(p, "rb") as f:
            data = f.read()
        with open(p, "wb") as f:  # sidecar .sha256 intentionally untouched
            f.write(_flip_byte(data))

    def db_rows(self, sql, params=()):
        return self.src.query(sql, params)

    def db_write(self, sql, params=()):
        import sqlite3
        c = sqlite3.connect(self.db_path, timeout=10)
        try:
            c.execute("PRAGMA foreign_keys = ON")
            c.execute(sql, params)
            c.commit()
        finally:
            c.close()

    def controller_rss_kb(self):
        p = self.procs.get("controller")
        if not p:
            return None
        out = sh("ps", "-o", "rss=", "-p", str(p.pid), check=False).stdout.strip()
        return int(out) if out else None


class DockerBackend:
    name = "docker"
    repair_grace = float(os.environ.get("REPAIR_GRACE_PERIOD", "30"))
    read_timeout = float(os.environ.get("WORKER_READ_TIMEOUT", "10"))

    def __init__(self):
        self.src = ci.DockerSource()

    def compose(self, *args, **kw):
        return sh("docker", "compose", *args, cwd=ROOT, **kw)

    def up(self):
        self.compose("up", "-d", "--build")

    def after_up(self):
        pass

    def down(self):
        self.compose("down")  # no -v: volumes survive

    def cleanup(self):
        pass  # leave the stack running for inspection

    def stop_worker(self, name):
        sh("docker", "stop", name)

    def start_worker_and_fix(self, name):
        sh("docker", "start", name)

    def pause_worker(self, name):
        sh("docker", "pause", name)

    def unpause_worker(self, name):
        sh("docker", "unpause", name)

    def restart_controller(self):
        sh("docker", "restart", "controller")

    def restart_workers(self, names):
        sh("docker", "restart", *names)

    def full_restart(self):
        self.down()
        self.up()

    def worker_files(self, worker):
        return self.src.worker_files(worker)

    def corrupt_chunk(self, worker, cid):
        code = ("import sys\np=f'/data/chunks/{sys.argv[1][:2]}/{sys.argv[1]}.chunk'\n"
                "b=bytearray(open(p,'rb').read()); b[len(b)//2]^=0xFF; open(p,'wb').write(bytes(b))")
        sh("docker", "exec", worker, "python", "-c", code, cid)

    def db_rows(self, sql, params=()):
        return self.src.query(sql, params)

    def db_write(self, sql, params=()):
        import json
        code = ("import sqlite3,sys,json\nc=sqlite3.connect('/data/controller.db',timeout=10)\n"
                "c.execute('PRAGMA foreign_keys = ON')\nc.execute(sys.argv[1], json.loads(sys.argv[2]))\nc.commit()")
        sh("docker", "exec", "controller", "python", "-c", code, sql, json.dumps(list(params)))

    def controller_rss_kb(self):
        r = sh("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", "controller", check=False)
        try:
            val = r.stdout.split("/")[0].strip()
            num = float("".join(c for c in val if c.isdigit() or c == "."))
            unit = "".join(c for c in val if c.isalpha()).lower()
            return int(num * {"kib": 1, "mib": 1024, "gib": 1024 * 1024, "b": 1 / 1024}.get(unit, 1))
        except Exception:
            return None


def choose_backend():
    mode = os.environ.get("DFS_BACKEND", "auto")
    if mode == "docker" or (mode == "auto" and ci.docker_usable()):
        return DockerBackend()
    return LocalBackend()
