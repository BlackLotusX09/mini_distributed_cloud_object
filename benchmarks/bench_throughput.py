"""Experiment 1: Upload and download throughput vs object size (1 KB to 1 GB).

Outputs:
- benchmarks/results/exp1_throughput_<ts>.csv (+ exp1_throughput_latest.csv)
- benchmarks/results/exp1_throughput_raw_<ts>.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h

DEFAULT_SIZES = ["1KB", "100KB", "1MB", "4MB", "16MB", "64MB", "256MB", "1GB"]


def run_experiment(
    sizes: List[str],
    warmup: int = 2,
    runs: int = 10,
    reset_cluster: bool = False,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    ts = h.utc_stamp()
    summary_rows: List[Dict] = []
    raw_rows: List[Dict] = []

    with h.make_client(timeout=900.0) as client:
        for size_label in sizes:
            size_bytes = h.parse_size(size_label)
            print(f"[Exp 1] Preparing payload {size_label} ({size_bytes} B)...", flush=True)
            payload = h.make_payload(size_bytes)
            expected_sha = h.sha256_hex(payload)

            # ---- Upload pass: warmup + measured, cleaning old objects between runs
            up_samples: List[h.RequestSample] = []
            keep_for_download: str | None = None

            total_up = warmup + runs
            for idx in range(total_up):
                sample = h.timed_upload(client, payload, filename=f"exp1_{size_label}_{idx}.bin")
                is_measured = idx >= warmup
                if is_measured:
                    up_samples.append(sample)
                    mbps = (sample.byte_count / h.MB) / sample.elapsed_sec if sample.ok and sample.elapsed_sec > 0 else 0.0
                    raw_rows.append({
                        "size_label": size_label,
                        "size_bytes": size_bytes,
                        "op": "upload",
                        "iter": idx - warmup,
                        "elapsed_sec": round(sample.elapsed_sec, 6),
                        "throughput_mb_s": round(mbps, 4),
                        "status_code": sample.status_code,
                        "byte_count": sample.byte_count,
                        "error": sample.error or "",
                    })
                if sample.object_id:
                    if keep_for_download is not None:
                        h.delete_object(client, keep_for_download)
                    keep_for_download = sample.object_id

            if keep_for_download is None:
                # Fallback attempt outside measurement if all upload attempts failed
                fallback = h.timed_upload(client, payload, filename=f"exp1_{size_label}_fallback.bin")
                keep_for_download = fallback.object_id

            # ---- Download pass: warmup + measured with post-timer SHA-256 verification
            dl_samples: List[h.RequestSample] = []
            checksum_failures = 0
            if keep_for_download is not None:
                for idx in range(warmup + runs):
                    sample = h.timed_download(
                        client,
                        keep_for_download,
                        expected_sha256=expected_sha,
                        verify_sha256=True,
                    )
                    if idx >= warmup:
                        dl_samples.append(sample)
                        if sample.checksum_ok is False:
                            checksum_failures += 1
                        mbps = (sample.byte_count / h.MB) / sample.elapsed_sec if sample.ok and sample.elapsed_sec > 0 else 0.0
                        raw_rows.append({
                            "size_label": size_label,
                            "size_bytes": size_bytes,
                            "op": "download",
                            "iter": idx - warmup,
                            "elapsed_sec": round(sample.elapsed_sec, 6),
                            "throughput_mb_s": round(mbps, 4),
                            "status_code": sample.status_code,
                            "byte_count": sample.byte_count,
                            "error": sample.error or "",
                        })
                h.delete_object(client, keep_for_download)
            else:
                for idx in range(runs):
                    s = h.RequestSample(0.0, 0, size_bytes, error="no uploaded object for download")
                    dl_samples.append(s)
                    raw_rows.append({
                        "size_label": size_label,
                        "size_bytes": size_bytes,
                        "op": "download",
                        "iter": idx,
                        "elapsed_sec": 0.0,
                        "throughput_mb_s": 0.0,
                        "status_code": 0,
                        "byte_count": size_bytes,
                        "error": s.error or "",
                    })

            del payload  # release large buffer before next size

            for op, samples, chk_fail in (("upload", up_samples, 0), ("download", dl_samples, checksum_failures)):
                lat_stats = h.summarize(samples)
                tp_vals = [
                    (s.byte_count / h.MB) / s.elapsed_sec
                    for s in samples if s.ok and s.elapsed_sec > 0
                ]
                tp_stats = h.summarize(tp_vals)
                summary_rows.append({
                    "size_label": size_label,
                    "size_bytes": size_bytes,
                    "op": op,
                    "warmup": warmup,
                    "runs": runs,
                    "ok_count": lat_stats["ok_count"],
                    "error_count": lat_stats["error_count"],
                    "checksum_failures": chk_fail,
                    "throughput_mb_s_median": round(tp_stats["median"], 4),
                    "throughput_mb_s_mean": round(tp_stats["mean"], 4),
                    "throughput_mb_s_stdev": round(tp_stats["stdev"], 4),
                    "throughput_mb_s_iqr": round(tp_stats["iqr"], 4),
                    "latency_s_median": round(lat_stats["median"], 6),
                    "latency_s_mean": round(lat_stats["mean"], 6),
                    "latency_s_min": round(lat_stats["min"], 6),
                    "latency_s_max": round(lat_stats["max"], 6),
                    "latency_s_stdev": round(lat_stats["stdev"], 6),
                    "latency_s_p95": round(lat_stats["p95"], 6),
                    "latency_s_p99": round(lat_stats["p99"], 6),
                })
                print(
                    f"  {op:8s} {size_label:>5s}: median={tp_stats['median']:7.2f} MB/s "
                    f"mean={tp_stats['mean']:7.2f} MB/s p95={lat_stats['p95']:.4f}s "
                    f"errors={lat_stats['error_count']}",
                    flush=True,
                )

    params_str = f"sizes={','.join(sizes)};warmup={warmup};runs={runs}"
    h.write_csv(
        h.RESULTS_DIR / f"exp1_throughput_raw_{ts}.csv",
        raw_rows,
        experiment="exp1_throughput_raw",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp1_throughput_raw_latest.csv",
    )
    out = h.write_csv(
        h.RESULTS_DIR / f"exp1_throughput_{ts}.csv",
        summary_rows,
        experiment="exp1_throughput",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp1_throughput_latest.csv",
    )
    print(f"[Exp 1] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 1: Throughput vs Object Size")
    parser.add_argument("--sizes", default=",".join(DEFAULT_SIZES), help="Comma-separated sizes")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--fresh-cluster", action="store_true")
    args = parser.parse_args()
    sizes = [s.strip() for s in args.sizes.split(",") if s.strip()]
    run_experiment(sizes, warmup=args.warmup, runs=args.runs, reset_cluster=args.fresh_cluster)


if __name__ == "__main__":
    main()
