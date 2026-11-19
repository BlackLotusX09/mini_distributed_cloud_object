"""Shared benchmarking harness for the Distributed Object Store.

Methodology invariants:
- 1 MB = 1,048,576 bytes (1 MiB).
- Payloads use os.urandom() (incompressible; never zero-filled buffers) generated
  strictly outside time.perf_counter() sections.
- SHA-256 verification and JSON/disk overhead stay outside timed blocks.
- Failed requests are never dropped: status_code, error_count, and error details
  are preserved as first-class measurements.
"""
from __future__ import annotations

import csv
import hashlib
import math
import os
import platform
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx

KB = 1024
MB = 1_048_576  # Authoritative project unit: 1 MB = 1,048,576 bytes
GB = 1024 * MB

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = Path(__file__).resolve().parent / "results"
PLOTS_DIR = Path(__file__).resolve().parent / "plots"

DEFAULT_CONTROLLER_URL = os.environ.get("OBJSTORE_URL", "http://localhost:8000").rstrip("/")
WORKERS = ("worker1", "worker2", "worker3", "worker4")
WORKER_PORTS = {
    "worker1": 8001,
    "worker2": 8002,
    "worker3": 8003,
    "worker4": 8004,
}


def parse_size(spec: str) -> int:
    """Parse human sizes like '1KB', '100KB', '16MB', '1GB', or integer bytes."""
    s = str(spec).strip().upper()
    units = (("GB", GB), ("GIB", GB), ("MB", MB), ("MIB", MB), ("KB", KB), ("KIB", KB), ("B", 1))
    for suffix, mult in units:
        if s.endswith(suffix):
            num = s[: -len(suffix)].strip()
            return int(float(num) * mult)
    return int(s)


def format_size(num_bytes: int) -> str:
    if num_bytes >= GB and num_bytes % GB == 0:
        return f"{num_bytes // GB}GB"
    if num_bytes >= MB and num_bytes % MB == 0:
        return f"{num_bytes // MB}MB"
    if num_bytes >= KB and num_bytes % KB == 0:
        return f"{num_bytes // KB}KB"
    if num_bytes >= GB:
        return f"{num_bytes / GB:.2f}GB"
    if num_bytes >= MB:
        return f"{num_bytes / MB:.2f}MB"
    if num_bytes >= KB:
        return f"{num_bytes / KB:.2f}KB"
    return f"{num_bytes}B"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_payload(size: int) -> bytes:
    """Pre-generate incompressible random bytes strictly outside timed loops."""
    if size < 0:
        raise ValueError("size must be >= 0")
    return os.urandom(size)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class RequestSample(tuple):
    """Unpacks as (elapsed_sec, status_code, byte_count) while exposing metadata."""

    def __new__(
        cls,
        elapsed_sec: float,
        status_code: int,
        byte_count: int,
        *,
        object_id: Optional[str] = None,
        checksum: Optional[str] = None,
        sha256: Optional[str] = None,
        checksum_ok: Optional[bool] = None,
        error: Optional[str] = None,
    ):
        inst = super().__new__(cls, (float(elapsed_sec), int(status_code), int(byte_count)))
        inst.elapsed_sec = float(elapsed_sec)
        inst.status_code = int(status_code)
        inst.byte_count = int(byte_count)
        inst.object_id = object_id
        inst.checksum = checksum
        inst.sha256 = sha256
        inst.checksum_ok = checksum_ok
        inst.error = error
        return inst

    @property
    def ok(self) -> bool:
        return (200 <= self.status_code < 300) and (self.error is None) and (self.checksum_ok is not False)


def make_client(base_url: str = DEFAULT_CONTROLLER_URL, timeout: float = 600.0,
                max_connections: int = 64) -> httpx.Client:
    return httpx.Client(
        base_url=base_url,
        timeout=httpx.Timeout(connect=10.0, read=timeout, write=timeout, pool=60.0),
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=min(32, max_connections),
        ),
    )


def timed_upload(
    client: httpx.Client,
    payload: bytes,
    filename: str = "bench.bin",
    metadata_json: Optional[str] = None,
) -> RequestSample:
    """Upload `payload` via POST /api/v1/objects with timing strictly around network I/O."""
    files = {"file": (filename, payload, "application/octet-stream")}
    data = {"metadata": metadata_json} if metadata_json is not None else None

    t0 = time.perf_counter()
    try:
        resp = client.post("/api/v1/objects", files=files, data=data)
        elapsed = time.perf_counter() - t0
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        return RequestSample(elapsed, 0, len(payload), error=f"{type(exc).__name__}: {exc}")

    obj_id: Optional[str] = None
    checksum: Optional[str] = None
    err: Optional[str] = None
    if 200 <= resp.status_code < 300:
        try:
            body = resp.json()
            obj_id = body.get("object_id")
            checksum = body.get("checksum")
        except Exception as exc:
            err = f"invalid JSON response: {exc}"
    else:
        err = resp.text[:250]

    return RequestSample(
        elapsed,
        resp.status_code,
        len(payload),
        object_id=obj_id,
        checksum=checksum,
        error=err,
    )


def timed_download(
    client: httpx.Client,
    object_id: str,
    *,
    expected_sha256: Optional[str] = None,
    verify_sha256: bool = False,
) -> RequestSample:
    """Download an object with timing strictly around the HTTP stream lifecycle.

    All chunk bytes are received inside the timer; SHA-256 verification happens
    strictly AFTER time.perf_counter() completes.
    """
    chunks: List[bytes] = []
    byte_count = 0
    status = 0
    err: Optional[str] = None

    t0 = time.perf_counter()
    try:
        with client.stream("GET", f"/api/v1/objects/{object_id}") as resp:
            status = resp.status_code
            for part in resp.iter_bytes(chunk_size=MB):
                chunks.append(part)
                byte_count += len(part)
        elapsed = time.perf_counter() - t0
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        return RequestSample(elapsed, status, byte_count, object_id=object_id,
                             error=f"{type(exc).__name__}: {exc}")

    if not (200 <= status < 300):
        raw_err = b"".join(chunks).decode("utf-8", errors="replace")[:250]
        chunks.clear()
        return RequestSample(elapsed, status, byte_count, object_id=object_id, error=raw_err)

    digest: Optional[str] = None
    checksum_ok: Optional[bool] = None
    if verify_sha256 or expected_sha256 is not None:
        h = hashlib.sha256()
        for part in chunks:
            h.update(part)
        digest = h.hexdigest()
        if expected_sha256 is not None:
            checksum_ok = (digest == expected_sha256)
            if not checksum_ok:
                err = f"sha256 mismatch: expected {expected_sha256[:16]}, got {digest[:16]}"
    chunks.clear()

    return RequestSample(
        elapsed,
        status,
        byte_count,
        object_id=object_id,
        sha256=digest,
        checksum_ok=checksum_ok,
        error=err,
    )


def delete_object(client: httpx.Client, object_id: str) -> int:
    try:
        resp = client.delete(f"/api/v1/objects/{object_id}")
        return resp.status_code
    except Exception:
        return 0


def repeat(fn: Callable[[], Any], n: int, warmup: int = 0) -> List[Any]:
    """Run `warmup` discarded iterations, then `n` measured iterations."""
    for _ in range(max(0, warmup)):
        fn()
    return [fn() for _ in range(max(0, n))]


def _percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = (q / 100.0) * (len(sorted_vals) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo]))


def summarize(samples: Sequence[Any]) -> Dict[str, Any]:
    """Compute distribution stats without hiding failed samples."""
    total = len(samples)
    ok_vals: List[float] = []
    errors = 0

    for item in samples:
        if isinstance(item, RequestSample):
            if item.ok:
                ok_vals.append(item.elapsed_sec)
            else:
                errors += 1
        elif isinstance(item, (tuple, list)) and len(item) >= 2:
            elapsed, status = float(item[0]), int(item[1])
            if 200 <= status < 300:
                ok_vals.append(elapsed)
            else:
                errors += 1
        else:
            val = float(item)
            if math.isfinite(val):
                ok_vals.append(val)
            else:
                errors += 1

    if not ok_vals:
        return {
            "count": total,
            "ok_count": 0,
            "error_count": errors,
            "median": 0.0,
            "mean": 0.0,
            "min": 0.0,
            "max": 0.0,
            "stdev": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "iqr": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }

    s = sorted(ok_vals)
    p25 = _percentile(s, 25.0)
    p75 = _percentile(s, 75.0)
    return {
        "count": total,
        "ok_count": len(s),
        "error_count": errors,
        "median": float(statistics.median(s)),
        "mean": float(statistics.fmean(s)),
        "min": float(s[0]),
        "max": float(s[-1]),
        "stdev": float(statistics.stdev(s)) if len(s) >= 2 else 0.0,
        "p25": p25,
        "p75": p75,
        "iqr": p75 - p25,
        "p95": _percentile(s, 95.0),
        "p99": _percentile(s, 99.0),
    }


# ------------------------------------------------------------------ metadata & CSV
_CACHED_ENV: Optional[Dict[str, str]] = None


def _cmd_out(cmd: Sequence[str], default: str = "") -> str:
    try:
        out = subprocess.check_output(cmd, cwd=REPO_ROOT, stderr=subprocess.DEVNULL, text=True)
        return out.strip()
    except Exception:
        return default


def get_git_commit() -> str:
    commit = _cmd_out(["git", "rev-parse", "--short", "HEAD"], "unknown")
    dirty = _cmd_out(["git", "status", "--porcelain"])
    return f"{commit}{'-dirty' if dirty else ''}"


def get_dfs_config() -> str:
    return (
        "CHUNK_SIZE=1048576;R=2;HEARTBEAT_TIMEOUT=15s;LIVENESS_CHECK_INTERVAL=3s;"
        "REPAIR_GRACE_PERIOD=30s;REPAIR_INTERVAL=10s;MAX_REPAIRS_PER_CYCLE=20;"
        "REPAIR_CONCURRENCY=4;SCRUB_INTERVAL=300s;WORKER_SCRUB_YIELD_DELAY=0.005s"
    )


def get_env_summary() -> str:
    global _CACHED_ENV
    if _CACHED_ENV is not None:
        return _CACHED_ENV["env_summary"]

    cpu = _cmd_out(["sysctl", "-n", "machdep.cpu.brand_string"], platform.processor() or "unknown")
    host_cpus = _cmd_out(["sysctl", "-n", "hw.ncpu"], str(os.cpu_count() or "?"))
    host_mem_raw = _cmd_out(["sysctl", "-n", "hw.memsize"], "0")
    try:
        host_mem_gib = f"{int(host_mem_raw) / (1024 ** 3):.1f}GiB" if int(host_mem_raw) > 0 else "?"
    except ValueError:
        host_mem_gib = "?"
    os_ver = f"{platform.system()} {platform.mac_ver()[0] or platform.release()}"

    docker_ncpu = _cmd_out(["docker", "info", "--format", "{{.NCPU}}"], "?")
    docker_mem = _cmd_out(["docker", "info", "--format", "{{.MemTotal}}"], "0")
    try:
        docker_mem_gib = f"{int(docker_mem) / (1024 ** 3):.1f}GiB" if int(docker_mem) > 0 else "?"
    except ValueError:
        docker_mem_gib = "?"
    docker_os = _cmd_out(["docker", "info", "--format", "{{.OperatingSystem}}"], "DockerDesktopVM")

    summary = (
        f"Host={cpu}({host_cpus}c,{host_mem_gib}) | OS={os_ver} | "
        f"DockerVM={docker_os}({docker_ncpu}c,{docker_mem_gib})"
    )
    _CACHED_ENV = {"env_summary": summary}
    return summary


def write_csv(
    path: str | Path,
    rows: List[Dict[str, Any]],
    *,
    experiment: str = "",
    params: str = "",
    latest_path: Optional[str | Path] = None,
) -> Path:
    """Write rows with provenance columns (timestamp, git_commit, experiment, params, config, env_summary)."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        out_path.write_text("")
        return out_path

    ts = utc_iso()
    git_commit = get_git_commit()
    cfg = get_dfs_config()
    env_summary = get_env_summary()

    meta_cols = ["timestamp", "git_commit", "experiment", "params", "config", "env_summary"]
    data_cols: List[str] = []
    for row in rows:
        for k in row.keys():
            if k not in meta_cols and k not in data_cols:
                data_cols.append(k)
    fieldnames = data_cols + meta_cols

    enriched: List[Dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        r.setdefault("timestamp", ts)
        r.setdefault("git_commit", git_commit)
        r.setdefault("experiment", experiment)
        r.setdefault("params", params)
        r.setdefault("config", cfg)
        r.setdefault("env_summary", env_summary)
        enriched.append(r)

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched)

    if latest_path is not None:
        lp = Path(latest_path)
        lp.parent.mkdir(parents=True, exist_ok=True)
        lp.write_text(out_path.read_text(encoding="utf-8"), encoding="utf-8")

    return out_path


# ------------------------------------------------------------------ cluster helpers
def wait_until(
    predicate: Callable[[], Any],
    timeout: float = 90.0,
    interval: float = 0.5,
    description: str = "condition",
) -> Any:
    deadline = time.monotonic() + timeout
    last_exc: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            res = predicate()
            if res:
                return res
            last_exc = None
        except Exception as exc:
            if isinstance(exc, PermissionError) or "Operation not permitted" in str(exc):
                raise
            last_exc = exc
        time.sleep(interval)
    detail = f" (last error: {last_exc})" if last_exc else ""
    raise TimeoutError(f"Timed out after {timeout:.1f}s waiting for {description}{detail}")


def _all_services_healthy(base_url: str = DEFAULT_CONTROLLER_URL) -> bool:
    with httpx.Client(timeout=2.5) as c:
        r = c.get(f"{base_url}/health")
        if r.status_code != 200 or r.json().get("active_workers") != len(WORKERS):
            return False
        for wid, port in WORKER_PORTS.items():
            wr = c.get(f"http://localhost:{port}/health")
            if wr.status_code != 200 or wr.json().get("worker_id") != wid:
                return False
        aw = c.get(f"{base_url}/api/v1/admin/workers")
        if aw.status_code != 200:
            return False
        statuses = {w["worker_id"]: w["status"] for w in aw.json()}
        return all(statuses.get(w) == "ALIVE" for w in WORKERS)


def ensure_cluster_healthy(timeout: float = 90.0, base_url: str = DEFAULT_CONTROLLER_URL) -> None:
    for wid in WORKERS:
        try:
            st = subprocess.check_output(
                ["docker", "inspect", "-f", "{{.State.Running}}", wid],
                cwd=REPO_ROOT, stderr=subprocess.DEVNULL, text=True,
            ).strip()
            if st != "true":
                subprocess.run(["docker", "compose", "start", wid], cwd=REPO_ROOT, check=True)
        except Exception:
            pass
    wait_until(lambda: _all_services_healthy(base_url), timeout=timeout, interval=0.5,
               description="controller + 4 ALIVE workers")


def fresh_cluster(build: bool = False, timeout: float = 120.0,
                  base_url: str = DEFAULT_CONTROLLER_URL) -> None:
    """Reset volumes and launch a clean 5-container Docker Compose cluster."""
    subprocess.run(["docker", "compose", "down", "-v"], cwd=REPO_ROOT, check=True)
    up_cmd = ["docker", "compose", "up", "-d"]
    if build:
        up_cmd.append("--build")
    subprocess.run(up_cmd, cwd=REPO_ROOT, check=True)
    wait_until(lambda: _all_services_healthy(base_url), timeout=timeout, interval=0.5,
               description="fresh cluster healthy with 4 ALIVE workers")


def get_replication_summary(client: Optional[httpx.Client] = None,
                            base_url: str = DEFAULT_CONTROLLER_URL) -> dict:
    if client is not None:
        resp = client.get("/api/v1/admin/replication")
        resp.raise_for_status()
        return resp.json()
    with make_client(base_url, timeout=15.0) as c:
        resp = c.get("/api/v1/admin/replication")
        resp.raise_for_status()
        return resp.json()


def get_workers(client: Optional[httpx.Client] = None,
                base_url: str = DEFAULT_CONTROLLER_URL) -> List[dict]:
    if client is not None:
        resp = client.get("/api/v1/admin/workers")
        resp.raise_for_status()
        return resp.json()
    with make_client(base_url, timeout=15.0) as c:
        resp = c.get("/api/v1/admin/workers")
        resp.raise_for_status()
        return resp.json()


def cleanup_objects(
    client: Optional[httpx.Client] = None,
    object_ids: Optional[Iterable[str]] = None,
    base_url: str = DEFAULT_CONTROLLER_URL,
) -> int:
    own = client is None
    c = make_client(base_url, timeout=60.0) if own else client
    deleted = 0
    try:
        if object_ids is not None:
            for oid in list(object_ids):
                if delete_object(c, oid) in (200, 204):
                    deleted += 1
            return deleted
        while True:
            resp = c.get("/api/v1/objects", params={"limit": 200, "offset": 0})
            if resp.status_code != 200:
                break
            objs = resp.json().get("objects", [])
            if not objs:
                break
            for o in objs:
                delete_object(c, o["object_id"])
                deleted += 1
        return deleted
    finally:
        if own:
            c.close()


def stop_worker(worker_id: str) -> float:
    t0 = time.time()
    subprocess.run(["docker", "stop", worker_id], cwd=REPO_ROOT, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return t0


def start_worker(worker_id: str) -> float:
    t0 = time.time()
    subprocess.run(["docker", "start", worker_id], cwd=REPO_ROOT, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return t0


def worker_du_bytes(worker_id: str) -> int:
    out = subprocess.check_output(
        ["docker", "exec", worker_id, "du", "-sb", "/data"],
        cwd=REPO_ROOT, text=True,
    ).strip()
    return int(out.split()[0])


def cluster_du_bytes() -> Dict[str, int]:
    return {wid: worker_du_bytes(wid) for wid in WORKERS}


def _parse_mem_mib(mem_usage_field: str) -> float:
    used = mem_usage_field.split("/", 1)[0].strip()
    for suffix, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1.0 / 1024.0),
                           ("GB", 953.674), ("MB", 0.953674), ("kB", 0.000953674), ("B", 1.0 / MB)):
        if used.endswith(suffix):
            try:
                return float(used[: -len(suffix)].strip()) * factor
            except ValueError:
                return 0.0
    return 0.0


def capture_docker_stats(containers: Sequence[str] = ("controller", *WORKERS)) -> List[Dict[str, Any]]:
    try:
        out = subprocess.check_output(
            [
                "docker", "stats", "--no-stream", "--format",
                "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}",
                *containers,
            ],
            cwd=REPO_ROOT, text=True, stderr=subprocess.DEVNULL,
        )
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    for line in out.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        name, cpu_s, mem_usage, mem_perc_s, net_io, block_io = parts[:6]
        try:
            cpu_pct = float(cpu_s.rstrip("%").strip())
        except ValueError:
            cpu_pct = 0.0
        try:
            mem_pct = float(mem_perc_s.rstrip("%").strip())
        except ValueError:
            mem_pct = 0.0
        rows.append({
            "container": name,
            "cpu_pct": cpu_pct,
            "mem_mib": round(_parse_mem_mib(mem_usage), 2),
            "mem_pct": mem_pct,
            "mem_usage": mem_usage,
            "net_io": net_io,
            "block_io": block_io,
        })
    return rows
