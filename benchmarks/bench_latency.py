"""Experiment 2: Small-object latency distribution (1 KB, 10 KB, 100 KB; 500 seq reqs each).

Outputs:
- benchmarks/results/exp2_latency_<ts>.csv (+ exp2_latency_latest.csv)
- benchmarks/results/exp2_latency_raw_<ts>.csv (+ exp2_latency_raw_latest.csv)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h

DEFAULT_SIZES = ["1KB", "10KB", "100KB"]


def run_experiment(
    sizes: List[str],
    runs: int = 500,
    warmup: int = 10,
    reset_cluster: bool = False,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    ts = h.utc_stamp()
    summary_rows: List[Dict] = []
    raw_rows: List[Dict] = []

    with h.make_client(timeout=60.0) as client:
        for size_label in sizes:
            size_bytes = h.parse_size(size_label)
            payload = h.make_payload(size_bytes)
            expected_sha = h.sha256_hex(payload)
            created_ids: List[str] = []

            print(f"[Exp 2] Running {warmup} warmup + {runs} sequential uploads & downloads for {size_label}...",
                  flush=True)

            # ---- Upload loop
            def _do_upload():
                s = h.timed_upload(client, payload, filename=f"exp2_{size_label}.bin")
                if s.object_id:
                    created_ids.append(s.object_id)
                return s

            up_samples = h.repeat(_do_upload, n=runs, warmup=warmup)
            for idx, s in enumerate(up_samples):
                raw_rows.append({
                    "size_label": size_label,
                    "size_bytes": size_bytes,
                    "op": "upload",
                    "iter": idx,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "byte_count": s.byte_count,
                    "error": s.error or "",
                })

            target_id = created_ids[-1] if created_ids else None
            if target_id is None:
                fallback = h.timed_upload(client, payload, filename=f"exp2_{size_label}_ref.bin")
                target_id = fallback.object_id
                if target_id:
                    created_ids.append(target_id)

            # ---- Download loop
            if target_id is not None:
                def _do_download():
                    return h.timed_download(client, target_id, expected_sha256=expected_sha, verify_sha256=True)

                dl_samples = h.repeat(_do_download, n=runs, warmup=warmup)
            else:
                dl_samples = [
                    h.RequestSample(0.0, 0, size_bytes, error="upload failed; no target object_id")
                    for _ in range(runs)
                ]

            for idx, s in enumerate(dl_samples):
                raw_rows.append({
                    "size_label": size_label,
                    "size_bytes": size_bytes,
                    "op": "download",
                    "iter": idx,
                    "latency_ms": round(s.elapsed_sec * 1000.0, 4),
                    "status_code": s.status_code,
                    "byte_count": s.byte_count,
                    "error": s.error or "",
                })

            # Cleanup objects outside timed loops
            h.cleanup_objects(client, created_ids)

            for op, samples in (("upload", up_samples), ("download", dl_samples)):
                stats = h.summarize(samples)
                summary_rows.append({
                    "size_label": size_label,
                    "size_bytes": size_bytes,
                    "op": op,
                    "warmup": warmup,
                    "runs": runs,
                    "ok_count": stats["ok_count"],
                    "error_count": stats["error_count"],
                    "median_ms": round(stats["median"] * 1000.0, 4),
                    "mean_ms": round(stats["mean"] * 1000.0, 4),
                    "min_ms": round(stats["min"] * 1000.0, 4),
                    "max_ms": round(stats["max"] * 1000.0, 4),
                    "stdev_ms": round(stats["stdev"] * 1000.0, 4),
                    "p25_ms": round(stats["p25"] * 1000.0, 4),
                    "p75_ms": round(stats["p75"] * 1000.0, 4),
                    "iqr_ms": round(stats["iqr"] * 1000.0, 4),
                    "p95_ms": round(stats["p95"] * 1000.0, 4),
                    "p99_ms": round(stats["p99"] * 1000.0, 4),
                })
                print(
                    f"  {op:8s} {size_label:>5s}: median={stats['median']*1000:.2f}ms "
                    f"p95={stats['p95']*1000:.2f}ms p99={stats['p99']*1000:.2f}ms "
                    f"errors={stats['error_count']}",
                    flush=True,
                )

    params_str = f"sizes={','.join(sizes)};warmup={warmup};runs={runs}"
    h.write_csv(
        h.RESULTS_DIR / f"exp2_latency_raw_{ts}.csv",
        raw_rows,
        experiment="exp2_latency_raw",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp2_latency_raw_latest.csv",
    )
    out = h.write_csv(
        h.RESULTS_DIR / f"exp2_latency_{ts}.csv",
        summary_rows,
        experiment="exp2_latency",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp2_latency_latest.csv",
    )
    print(f"[Exp 2] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 2: Small-Object Sequential Latency")
    parser.add_argument("--sizes", default=",".join(DEFAULT_SIZES))
    parser.add_argument("--runs", type=int, default=500)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--fresh-cluster", action="store_true")
    args = parser.parse_args()
    sizes = [s.strip() for s in args.sizes.split(",") if s.strip()]
    run_experiment(sizes, runs=args.runs, warmup=args.warmup, reset_cluster=args.fresh_cluster)


if __name__ == "__main__":
    main()
