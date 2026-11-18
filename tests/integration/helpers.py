"""Deterministic polling + data helpers shared by the integration suites."""
import hashlib
import os
import time


def wait_for(predicate_fn, timeout: float = 30.0, interval: float = 0.25, desc: str = ""):
    """Poll until predicate_fn() is truthy; raise TimeoutError with the last observed state."""
    deadline = time.monotonic() + timeout
    last = None
    while True:
        try:
            last = predicate_fn()
        except Exception as e:  # transient connection errors while a node restarts
            last = f"{type(e).__name__}: {e}"
        else:
            if last:
                return last
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out after {timeout}s waiting for: {desc or predicate_fn!r}; last state: {last!r}")
        time.sleep(interval)


def random_bytes(n: int) -> bytes:
    return os.urandom(n)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def expect(resp, status, what=""):
    """Assert status code with a diagnostic message."""
    assert resp.status_code == status, f"Expected {status}, got {resp.status_code}: {resp.text[:500]} {what}"
    return resp


def expect_error(resp, status, code):
    expect(resp, status)
    assert resp.headers["content-type"].startswith("application/json"), resp.headers
    body = resp.json()
    assert set(body) == {"error", "message"}, body
    assert body["error"] == code, f"Expected error {code}, got {body}"
    return body


def workers_state(client):
    return {w["worker_id"]: w for w in client.get("/api/v1/admin/workers").json()}


def wait_worker(client, name, status, timeout=60.0):
    return wait_for(lambda: workers_state(client).get(name, {}).get("status") == status,
                    timeout=timeout, desc=f"{name} -> {status} (now {workers_state(client).get(name, {}).get('status')})")


def get_chunk_map(client, object_id):
    """[{index, chunk_id, size, checksum, replicas:[{worker_id,state,worker_status}]}] from the admin API."""
    return expect(client.get(f"/api/v1/admin/objects/{object_id}/chunks"), 200).json()["chunks"]


def valid_workers(chunk):
    return sorted(r["worker_id"] for r in chunk["replicas"]
                  if r["state"] == "OK" and r["worker_status"] == "ALIVE")


def all_chunks_have(client, object_id, n=2, exclude=()):
    cm = get_chunk_map(client, object_id)
    return bool(cm) and all(len(valid_workers(c)) == n and not set(valid_workers(c)) & set(exclude) for c in cm)
