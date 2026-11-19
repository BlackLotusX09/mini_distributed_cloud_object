"""Experiment 3: Concurrency scaling (1, 2, 4, 8, 16 clients @ 16 MB) + docker stats capture.

Outputs:
- benchmarks/results/exp3_concurrency_<ts>.csv (+ exp3_concurrency_latest.csv)
- benchmarks/results/exp3_concurrency_raw_<ts>.csv
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h


def _stats_sampler(stop_event: threading.Event, snapshots: List[List[Dict]], interval: float = 0.75) -> None:
    while not stop_event.is_set():
        snap = h.capture_docker_stats()
        if snap:
            snapshots.append(snap)
        stop_event.wait(interval)


def _aggregate_container_stats(snapshots: List[List[Dict]]) -> Dict[str, float]:
    by_name: Dict[str, List[Dict]] = {}
    for snap in snapshots:
        for row in snap:
            by_name.setdefault(row["container"], []).append(row)

    ctrl = by_name.get("controller", [])
    ctrl_cpu = sum(r["cpu_pct"] for r in ctrl) / len(ctrl) if ctrl else 0.0
    ctrl_mem = max((r["mem_mib"] for r in ctrl), default=0.0)

    worker_cpus: List[float] = []
    worker_mems: List[float] = []
    for wid in h.WORKERS:
        rows = by_name.get(wid, [])
        if rows:
            worker_cpus.append(sum(r["cpu_pct"] for r in rows) / len(rows))
            worker_mems.append(max(r["mem_mib"] for r in rows))

    return {
        "controller_cpu_pct": round(ctrl_cpu, 2),
        "controller_mem_mib": round(ctrl_mem, 2),
        "workers_avg_cpu_pct": round(sum(worker_cpus) / len(worker_cpus), 2) if worker_cpus else 0.0,
        "workers_sum_cpu_pct": round(sum(worker_cpus), 2),
        "workers_max_mem_mib": round(max(worker_mems, default=0.0), 2),
        "stats_samples": len(snapshots),
    }


def run_experiment(
    concurrencies: List[int],
    size_str: str = "16MB",
    requests_per_worker: int = 3,
    reset_cluster: bool = False,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    size_bytes = h.parse_size(size_str)
    payload = h.make_payload(size_bytes)
    expected_sha = h.sha256_hex(payload)
    ts = h.utc_stamp()

    summary_rows: List[Dict] = []
    raw_rows: List[Dict] = []

    with h.make_client(timeout=600.0) as admin_client:
        # Prepare a dedicated reference object per worker slot for contention-free download tests
        for conc in concurrencies:
            print(f"[Exp 3] Concurrency={conc}, size={size_str}, reqs/worker={requests_per_worker}...", flush=True)

            # ---- Upload pass
            up_samples: List[h.RequestSample] = []
            created_ids: List[str] = []
            lock = threading.Lock()
            barrier = threading.Barrier(conc)

            def _upload_worker(wid: int) -> None:
                with h.make_client(timeout=600.0, max_connections=8) as client:
                    barrier.wait()
                    for r in range(requests_per_worker):
                        sample = h.timed_upload(client, payload, filename=f"exp3_c{conc}_w{wid}_{r}.bin")
                        with lock:
                            up_samples.append(sample)
                            if sample.object_id:
                                created_ids.append(sample.object_id)

            snapshots_up: List[List[Dict]] = []
            stop_up = threading.Event()
            mon_up = threading.Thread(target=_stats_sampler, args=(stop_up, snapshots_up), daemon=True)
            mon_up.start()

            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=conc) as pool:
                futs = [pool.submit(_upload_worker, i) for i in range(conc)]
                for f in futs:
                    f.result()
            wall_up = time.perf_counter() - t0
            stop_up.set()
            mon_up.join(timeout=5.0)
            if not snapshots_up:
                snap = h.capture_docker_stats()
                if snap:
                    snapshots_up.append(snap)

            up_stats = h.summarize(up_samples)
            up_ok_bytes = sum(s.byte_count for s in up_samples if s.ok)
            up_agg_mbps = (up_ok_bytes / h.MB) / wall_up if wall_up > 0 else 0.0
            up_dstats = _aggregate_container_stats(snapshots_up)

            for idx, s in enumerate(up_samples):
                raw_rows.append({
                    "concurrency": conc,
                    "op": "upload",
                    "req_idx": idx,
                    "elapsed_sec": round(s.elapsed_sec, 6),
                    "status_code": s.status_code,
                    "byte_count": s.byte_count,
                    "error": s.error or "",
                })

            summary_rows.append({
                "concurrency": conc,
                "size_bytes": size_bytes,
                "op": "upload",
                "total_requests": len(up_samples),
                "ok_count": up_stats["ok_count"],
                "error_count": up_stats["error_count"],
                "wall_sec": round(wall_up, 4),
                "agg_throughput_mb_s": round(up_agg_mbps, 4),
                "req_latency_median_s": round(up_stats["median"], 4),
                "req_latency_p95_s": round(up_stats["p95"], 4),
                "req_latency_stdev_s": round(up_stats["stdev"], 4),
                **up_dstats,
            })
            print(
                f"  upload   c={conc:2d}: agg={up_agg_mbps:6.2f} MB/s wall={wall_up:5.2f}s "
                f"p95={up_stats['p95']:.3f}s err={up_stats['error_count']} "
                f"ctrl_cpu={up_dstats['controller_cpu_pct']}% wk_cpu_sum={up_dstats['workers_sum_cpu_pct']}%",
                flush=True,
            )

            # Guarantee at least one valid object for the download pass
            if not created_ids:
                ref = h.timed_upload(admin_client, payload, filename=f"exp3_ref_{conc}.bin")
                if ref.object_id:
                    created_ids.append(ref.object_id)

            # ---- Download pass
            dl_samples: List[h.RequestSample] = []
            dl_lock = threading.Lock()
            dl_barrier = threading.Barrier(conc)

            def _download_worker(wid: int) -> None:
                with h.make_client(timeout=600.0, max_connections=8) as client:
                    dl_barrier.wait()
                    for r in range(requests_per_worker):
                        oid = created_ids[(wid + r) % len(created_ids)]
                        sample = h.timed_download(client, oid, expected_sha256=expected_sha, verify_sha256=True)
                        with dl_lock:
                            dl_samples.append(sample)

            snapshots_dl: List[List[Dict]] = []
            stop_dl = threading.Event()
            mon_dl = threading.Thread(target=_stats_sampler, args=(stop_dl, snapshots_dl), daemon=True)
            mon_dl.start()

            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=conc) as pool:
                futs = [pool.submit(_download_worker, i) for i in range(conc)]
                for f in futs:
                    f.result()
            wall_dl = time.perf_counter() - t0
            stop_dl.set()
            mon_dl.join(timeout=5.0)
            if not snapshots_dl:
                snap = h.capture_docker_stats()
                if snap:
                    snapshots_dl.append(snap)

            dl_stats = h.summarize(dl_samples)
            dl_ok_bytes = sum(s.byte_count for s in dl_samples if s.ok)
            dl_agg_mbps = (dl_ok_bytes / h.MB) / wall_dl if wall_dl > 0 else 0.0
            dl_dstats = _aggregate_container_stats(snapshots_dl)

            for idx, s in enumerate(dl_samples):
                raw_rows.append({
                    "concurrency": conc,
                    "op": "download",
                    "req_idx": idx,
                    "elapsed_sec": round(s.elapsed_sec, 6),
                    "status_code": s.status_code,
                    "byte_count": s.byte_count,
                    "error": s.error or "",
                })

            summary_rows.append({
                "concurrency": conc,
                "size_bytes": size_bytes,
                "op": "download",
                "total_requests": len(dl_samples),
                "ok_count": dl_stats["ok_count"],
                "error_count": dl_stats["error_count"],
                "wall_sec": round(wall_dl, 4),
                "agg_throughput_mb_s": round(dl_agg_mbps, 4),
                "req_latency_median_s": round(dl_stats["median"], 4),
                "req_latency_p95_s": round(dl_stats["p95"], 4),
                "req_latency_stdev_s": round(dl_stats["stdev"], 4),
                **dl_dstats,
            })
            print(
                f"  download c={conc:2d}: agg={dl_agg_mbps:6.2f} MB/s wall={wall_dl:5.2f}s "
                f"p95={dl_stats['p95']:.3f}s err={dl_stats['error_count']} "
                f"ctrl_cpu={dl_dstats['controller_cpu_pct']}% wk_cpu_sum={dl_dstats['workers_sum_cpu_pct']}%",
                flush=True,
            )

            h.cleanup_objects(admin_client, created_ids)

    params_str = f"size={size_str};concurrencies={','.join(map(str, concurrencies))};reqs_per_worker={requests_per_worker}"
    h.write_csv(
        h.RESULTS_DIR / f"exp3_concurrency_raw_{ts}.csv",
        raw_rows,
        experiment="exp3_concurrency_raw",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp3_concurrency_raw_latest.csv",
    )
    out = h.write_csv(
        h.RESULTS_DIR / f"exp3_concurrency_{ts}.csv",
        summary_rows,
        experiment="exp3_concurrency",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp3_concurrency_latest.csv",
    )
    print(f"[Exp 3] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 3: Concurrency Scaling & Resource Bottlenecks")
    parser.add_argument("--size", default="16MB")
    parser.add_argument("--concurrency", default="1,2,4,8,16")
    parser.add_argument("--requests-per-worker", type=int, default=3)
    parser.add_argument("--fresh-cluster", action="store_true")
    args = parser.parse_args()
    concs = [int(x.strip()) for x in args.concurrency.split(",") if x.strip()]
    run_experiment(concs, size_str=args.size, requests_per_worker=args.requests_per_worker,
                   reset_cluster=args.fresh_cluster)


if __name__ == "__main__":
    main()
