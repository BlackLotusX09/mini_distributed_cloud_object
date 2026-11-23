"""Experiment 7: Scrubbing throughput (Idle vs Under Concurrent Download Load).

At R=2 across 4 workers, populating 2 GB logical data stores ~1 GB raw data per worker.
Triggers POST /api/v1/admin/scrub and measures duration and MB/s both idle and under
4-reader concurrent download load.

Outputs:
- benchmarks/results/exp7_scrub_<ts>.csv (+ exp7_scrub_latest.csv)
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


def run_experiment(
    per_worker_size_str: str = "1GB",
    object_size_str: str = "16MB",
    readers: int = 4,
    reset_cluster: bool = True,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    per_worker_bytes = h.parse_size(per_worker_size_str)
    # Cluster has 4 workers and R=2 -> total logical data = 2 * per_worker_bytes (~1 GB/worker raw)
    target_logical = 2 * per_worker_bytes
    obj_size = h.parse_size(object_size_str)
    count = max(1, target_logical // obj_size)

    payload = h.make_payload(obj_size)
    expected_sha = h.sha256_hex(payload)
    ts = h.utc_stamp()

    print(
        f"[Exp 7] Populating {count} x {object_size_str} ({count * obj_size / h.MB:.0f} MB logical, "
        f"~{per_worker_size_str} raw per worker at R=2)...",
        flush=True,
    )

    uploaded_ids: List[str] = []
    id_lock = threading.Lock()
    counter = 0
    cnt_lock = threading.Lock()

    def _uploader() -> None:
        nonlocal counter
        with h.make_client(timeout=180.0, max_connections=8) as client:
            while True:
                with cnt_lock:
                    if counter >= count:
                        return
                    idx = counter
                    counter += 1
                s = h.timed_upload(client, payload, filename=f"exp7_scrub_{idx:05d}.bin")
                if s.ok and s.object_id:
                    with id_lock:
                        uploaded_ids.append(s.object_id)

    with ThreadPoolExecutor(max_workers=4) as pool:
        for f in [pool.submit(_uploader) for _ in range(4)]:
            f.result()

    if not uploaded_ids:
        raise RuntimeError("Failed to upload test objects for scrub experiment")

    du_map = h.cluster_du_bytes()
    total_chunk_bytes = sum(w["valid_replicas"] * h.MB for w in h.get_replication_summary()["workers"])
    raw_du_total = sum(du_map.values())
    print(
        f"[Exp 7] Populated {len(uploaded_ids)} objects | total replica bytes={total_chunk_bytes / h.MB:.1f} MB "
        f"| per-worker du (MB): { {k: round(v / h.MB, 1) for k, v in du_map.items()} }",
        flush=True,
    )

    rows: List[Dict] = []

    def _run_scrub(mode: str, under_load: bool) -> None:
        stop_readers = threading.Event()
        dl_samples: List[h.RequestSample] = []
        dl_lock = threading.Lock()
        reader_threads: List[threading.Thread] = []

        if under_load:
            def _reader(rid: int) -> None:
                i = rid
                with h.make_client(timeout=60.0, max_connections=4) as c:
                    while not stop_readers.is_set():
                        oid = uploaded_ids[i % len(uploaded_ids)]
                        i += readers
                        s = h.timed_download(c, oid, expected_sha256=expected_sha, verify_sha256=True)
                        with dl_lock:
                            dl_samples.append(s)

            reader_threads = [threading.Thread(target=_reader, args=(r,), daemon=True) for r in range(readers)]
            for t in reader_threads:
                t.start()
            time.sleep(2.0)  # establish steady concurrent read load before triggering scrub

        with h.make_client(timeout=600.0) as scrub_client:
            t0 = time.perf_counter()
            resp = scrub_client.post("/api/v1/admin/scrub")
            elapsed = time.perf_counter() - t0
            status_code = resp.status_code
            body = resp.json() if status_code == 200 else {}

        if under_load:
            stop_readers.set()
            for t in reader_threads:
                t.join(timeout=60.0)

        checked = int(body.get("chunks_checked", 0))
        bytes_checked = total_chunk_bytes if total_chunk_bytes > 0 else checked * h.MB
        cluster_mb_s = (bytes_checked / h.MB) / elapsed if elapsed > 0 and status_code == 200 else 0.0
        workers_scrubbed = int(body.get("workers_scrubbed", 0))

        dl_stats = h.summarize(dl_samples) if dl_samples else {"ok_count": 0, "error_count": 0, "p95": 0.0, "median": 0.0}
        dl_bytes = sum(s.byte_count for s in dl_samples if s.ok)
        dl_mb_s = (dl_bytes / h.MB) / elapsed if (under_load and elapsed > 0) else 0.0

        rows.append({
            "mode": mode,
            "per_worker_target_bytes": per_worker_bytes,
            "raw_du_total_bytes": raw_du_total,
            "total_replica_bytes": bytes_checked,
            "status_code": status_code,
            "workers_scrubbed": workers_scrubbed,
            "chunks_checked": checked,
            "corrupt_found": int(body.get("corrupt_found", 0)),
            "scrub_duration_s": round(elapsed, 4),
            "cluster_scrub_mb_s": round(cluster_mb_s, 4),
            "concurrent_readers": readers if under_load else 0,
            "bg_downloads_ok": dl_stats["ok_count"],
            "bg_downloads_err": dl_stats["error_count"],
            "bg_download_median_s": round(dl_stats["median"], 4),
            "bg_download_p95_s": round(dl_stats["p95"], 4),
            "bg_approx_throughput_mb_s": round(dl_mb_s, 4),
        })
        print(
            f"  {mode:10s}: duration={elapsed:.2f}s chunks={checked} "
            f"scrub_rate={cluster_mb_s:.2f} MB/s corrupt={body.get('corrupt_found', 0)} "
            f"bg_ok={dl_stats['ok_count']} bg_err={dl_stats['error_count']}",
            flush=True,
        )

    _run_scrub("idle", under_load=False)
    time.sleep(2.0)
    _run_scrub("under_load", under_load=True)

    h.cleanup_objects(object_ids=uploaded_ids)
    params_str = f"per_worker_size={per_worker_size_str};object_size={object_size_str};readers={readers}"
    out = h.write_csv(
        h.RESULTS_DIR / f"exp7_scrub_{ts}.csv",
        rows,
        experiment="exp7_scrub",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp7_scrub_latest.csv",
    )
    print(f"[Exp 7] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 7: Background/Manual Scrub Throughput")
    parser.add_argument("--per-worker-size", default="1GB")
    parser.add_argument("--object-size", default="16MB")
    parser.add_argument("--readers", type=int, default=4)
    parser.add_argument("--no-reset", action="store_true")
    args = parser.parse_args()
    run_experiment(
        per_worker_size_str=args.per_worker_size,
        object_size_str=args.object_size,
        readers=args.readers,
        reset_cluster=not args.no_reset,
    )


if __name__ == "__main__":
    main()
