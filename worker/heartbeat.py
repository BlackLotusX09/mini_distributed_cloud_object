"""Background cluster-membership daemon: register (with backoff), then heartbeat."""
import json
import shutil
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone


def log(msg: str) -> None:
    print(msg, flush=True)


class HeartbeatDaemon(threading.Thread):
    def __init__(self, cfg, storage, base_backoff=1.0, max_backoff=10.0, timeout=3.0):
        super().__init__(daemon=True, name="heartbeat")
        self.cfg = cfg
        self.storage = storage
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self.timeout = timeout
        self._halt = threading.Event()

    def stop(self) -> None:
        self._halt.set()

    # -------------------------------------------------------------- wire
    def _stats(self) -> dict:
        try:
            free = shutil.disk_usage(self.cfg.data_dir).free
        except OSError:
            free = 0
        return {"free_bytes": free, "chunk_count": self.storage.chunk_count()}

    def _post(self, path: str, payload: dict) -> int:
        """POST JSON; return HTTP status (HTTP error statuses are returned, not raised)."""
        req = urllib.request.Request(
            self.cfg.controller_url + path,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def register(self) -> bool:
        payload = {"worker_id": self.cfg.worker_id, "address": self.cfg.address, **self._stats()}
        try:
            status = self._post("/internal/workers/register", payload)
        except Exception as e:  # refused, timeout, DNS ...
            log(f"[{self.cfg.worker_id}] register failed: {e}")
            return False
        if 200 <= status < 300:
            log(f"[{self.cfg.worker_id}] registered as {self.cfg.address}")
            return True
        log(f"[{self.cfg.worker_id}] register rejected: HTTP {status}")
        return False

    def beat(self) -> str:
        """Return 'ok', 'unknown' (controller doesn't know us) or 'error'."""
        payload = {
            "worker_id": self.cfg.worker_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **self._stats(),
        }
        try:
            status = self._post("/internal/workers/heartbeat", payload)
        except Exception as e:
            log(f"[{self.cfg.worker_id}] heartbeat failed: {e}")
            return "error"
        if 200 <= status < 300:
            return "ok"
        if status == 404:
            log(f"[{self.cfg.worker_id}] controller does not know us; re-registering")
            return "unknown"
        log(f"[{self.cfg.worker_id}] heartbeat rejected: HTTP {status}")
        return "error"

    # -------------------------------------------------------------- loop
    def _register_with_backoff(self) -> None:
        delay = self.base_backoff
        while not self._halt.is_set():
            if self.register():
                return
            self._halt.wait(delay)
            delay = min(delay * 2, self.max_backoff)

    def run(self) -> None:
        while not self._halt.is_set():
            self._register_with_backoff()
            while not self._halt.is_set():
                if self._halt.wait(self.cfg.heartbeat_interval):
                    return
                if self.beat() == "unknown":
                    break  # fall back to registration

