"""Experiment 4: Controller Overhead vs Direct Worker Access at 1 MB.

Compares persistent-session Controller operations (POST /api/v1/objects, GET /api/v1/objects/{id})
against direct Worker operations (PUT /internal/chunks/{id}, GET /internal/chunks/{id}), reporting
both 1x and 2x direct worker cost to account for R=2 replication.

Outputs:
- benchmarks/results/exp4_overhead_<ts>.csv (+ exp4_overhead_latest.csv)
- benchmarks/results/exp4_overhead_raw_<ts>.csv
"""
from __future__ import annotations

import argparse
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, List

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h


def _timed_worker_put(client: httpx.Client, chunk_id: str, payload: bytes, checksum: str) -> h.RequestSample:
    headers = {"Content-Length": str(len(payload)), "X-Chunk-Checksum": checksum}
    t0 = time.perf_counter()
    try:
        resp = client.put(f"/internal/chunks/{chunk_id}", content=payload, headers=headers)
        elapsed = time.perf_counter() - t0
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        return h.RequestSample(elapsed, 0, len(payload), error=f"{type(exc).__name__}: {exc}")
    err = None if 200 <= resp.status_code < 300 else resp.text[:250]
    return h.RequestSample(elapsed, resp.status_code, len(payload), error=err)


def _timed_worker_get(client: httpx.Client, chunk_id: str, expected_sha256: str) -> h.RequestSample:
    chunks: List[bytes] = []
    status = 0
    byte_count = 0
    t0 = time.perf_counter()
    try:
        with client.stream("GET", f"/internal/chunks/{chunk_id}") as resp:
            status = resp.status_code
            for part in resp.iter_bytes(chunk_size=h.MB):
                chunks.append(part)
                byte_count += len(part)
        elapsed = time.perf_counter() - t0
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        return h.RequestSample(elapsed, status, byte_count, error=f"{type(exc).__name__}: {exc}")

    if not (200 <= status < 300):
        return h.RequestSample(elapsed, status, byte_count, error=b"".join(chunks).decode("utf-8", "replace")[:250])

    import hashlib
    digest = hashlib.sha256(b"".join(chunks)).hexdigest()
    ok = digest == expected_sha256
    return h.RequestSample(elapsed, status, byte_count, sha256=digest, checksum_ok=ok,
                           error=None if ok else "sha256 mismatch")


def run_experiment(
    size_str: str = "1MB",
    warmup: int = 5,
    runs: int = 50,
    worker_port: int = 8001,
    reset_cluster: bool = False,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    size_bytes = h.parse_size(size_str)
    payload = h.make_payload(size_bytes)
    checksum = h.sha256_hex(payload)
    ts = h.utc_stamp()

    raw_rows: List[Dict] = []
    summary_rows: List[Dict] = []

    worker_url = f"http://localhost:{worker_port}"

    with h.make_client(timeout=60.0) as ctrl_client, h.make_client(worker_url, timeout=60.0) as wk_client:
        print(f"[Exp 4] Direct Worker ({worker_url}) vs Controller ({h.DEFAULT_CONTROLLER_URL}) @ {size_str} "
              f"(warmup={warmup}, runs={runs})...", flush=True)

        # ---- 1. Direct Worker PUT (unique chunk IDs so every request does full write + fsync)
        direct_cids: List[str] = []
        run_tag = uuid.uuid4().hex[:8]
        for idx in range(warmup + runs):
            cid = f"bench_exp4_{run_tag}_{idx:06d}"
            s = _timed_worker_put(wk_client, cid, payload, checksum)
            direct_cids.append(cid)
            if idx >= warmup:
                raw_rows.append({
                    "path": "direct_worker",
                    "op": "upload",
                    "iter": idx - warmup,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "error": s.error or "",
                })
        direct_put_samples = [
            h.RequestSample(r["latency_ms"] / 1000.0, r["status_code"], size_bytes, error=r["error"] or None)
            for r in raw_rows if r["path"] == "direct_worker" and r["op"] == "upload"
        ]

        # ---- 2. Direct Worker GET
        ref_cid = direct_cids[-1]
        direct_get_samples = []
        for idx in range(warmup + runs):
            s = _timed_worker_get(wk_client, ref_cid, checksum)
            if idx >= warmup:
                direct_get_samples.append(s)
                raw_rows.append({
                    "path": "direct_worker",
                    "op": "download",
                    "iter": idx - warmup,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "error": s.error or "",
                })

        # Remove direct chunks before controller pass so reconciliation/scrub stay pristine
        for cid in direct_cids:
            wk_client.delete(f"/internal/chunks/{cid}")

        # ---- 3. Controller Upload (POST /api/v1/objects)
        ctrl_oids: List[str] = []
        ctrl_up_samples: List[h.RequestSample] = []
        for idx in range(warmup + runs):
            s = h.timed_upload(ctrl_client, payload, filename=f"exp4_ctrl_{idx}.bin")
            if s.object_id:
                ctrl_oids.append(s.object_id)
            if idx >= warmup:
                ctrl_up_samples.append(s)
                raw_rows.append({
                    "path": "controller",
                    "op": "upload",
                    "iter": idx - warmup,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "error": s.error or "",
                })

        # ---- 4. Controller Download (GET /api/v1/objects/{id})
        ref_oid = ctrl_oids[-1]
        ctrl_dl_samples: List[h.RequestSample] = []
        for idx in range(warmup + runs):
            s = h.timed_download(ctrl_client, ref_oid, expected_sha256=checksum, verify_sha256=True)
            if idx >= warmup:
                ctrl_dl_samples.append(s)
                raw_rows.append({
                    "path": "controller",
                    "op": "download",
                    "iter": idx - warmup,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "error": s.error or "",
                })

        h.cleanup_objects(ctrl_client, ctrl_oids)

    # Summarize direct 1x, direct 2x (for R=2 baseline), and controller
    pairs = [
        ("upload", direct_put_samples, ctrl_up_samples),
        ("download", direct_get_samples, ctrl_dl_samples),
    ]
    for op, d_samples, c_samples in pairs:
        d_stats = h.summarize(d_samples)
        c_stats = h.summarize(c_samples)
        d1_med_ms = d_stats["median"] * 1000.0
        d2_med_ms = 2.0 * d1_med_ms
        c_med_ms = c_stats["median"] * 1000.0

        rows_for_op = [
            ("direct_1x", d_stats, 1.0, 0.0, 0.0),
            ("direct_2x", {k: (v * 2.0 if k in ("median", "mean", "min", "max", "stdev", "p95", "p99", "iqr") else v)
                           for k, v in d_stats.items()}, 2.0, 0.0, 0.0),
            (
                "controller",
                c_stats,
                1.0,
                c_med_ms - d1_med_ms,
                ((c_med_ms - d1_med_ms) / d1_med_ms * 100.0) if d1_med_ms > 0 else 0.0,
            ),
        ]
        for label, st, mult, ov1_ms, ov1_pct in rows_for_op:
            ov2_ms = (c_med_ms - d2_med_ms) if label == "controller" else 0.0
            ov2_pct = ((c_med_ms - d2_med_ms) / d2_med_ms * 100.0) if (label == "controller" and d2_med_ms > 0) else 0.0
            summary_rows.append({
                "op": op,
                "path": label,
                "multiplier": mult,
                "size_bytes": size_bytes,
                "runs": runs,
                "ok_count": st["ok_count"],
                "error_count": st["error_count"],
                "median_ms": round(st["median"] * 1000.0, 4),
                "mean_ms": round(st["mean"] * 1000.0, 4),
                "stdev_ms": round(st["stdev"] * 1000.0, 4),
                "p95_ms": round(st["p95"] * 1000.0, 4),
                "p99_ms": round(st["p99"] * 1000.0, 4),
                "overhead_vs_1x_ms": round(ov1_ms, 4),
                "overhead_vs_1x_pct": round(ov1_pct, 2),
                "overhead_vs_2x_ms": round(ov2_ms, 4),
                "overhead_vs_2x_pct": round(ov2_pct, 2),
            })
        print(
            f"  {op:8s}: direct_1x={d1_med_ms:.2f}ms  direct_2x={d2_med_ms:.2f}ms  "
            f"controller={c_med_ms:.2f}ms (vs 1x: +{c_med_ms-d1_med_ms:.2f}ms, vs 2x: {c_med_ms-d2_med_ms:+.2f}ms)",
            flush=True,
        )

    params_str = f"size={size_str};warmup={warmup};runs={runs};worker_port={worker_port}"
    h.write_csv(
        h.RESULTS_DIR / f"exp4_overhead_raw_{ts}.csv",
        raw_rows,
        experiment="exp4_overhead_raw",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp4_overhead_raw_latest.csv",
    )
    out = h.write_csv(
        h.RESULTS_DIR / f"exp4_overhead_{ts}.csv",
        summary_rows,
        experiment="exp4_overhead",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp4_overhead_latest.csv",
    )
    print(f"[Exp 4] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 4: Controller Overhead vs Direct Worker")
    parser.add_argument("--size", default="1MB")
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--worker-port", type=int, default=8001)
    parser.add_argument("--fresh-cluster", action="store_true")
    args = parser.parse_args()
    run_experiment(size_str=args.size, warmup=args.warmup, runs=args.runs,
                   worker_port=args.worker_port, reset_cluster=args.fresh_cluster)


if __name__ == "__main__":
    main()
