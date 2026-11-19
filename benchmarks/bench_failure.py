"""Experiment 5: Continuous 180s load during worker stop (t=60s) & rejoin (t=120s).

Runs 4 concurrent readers + 1 periodic writer while stopping worker2 at t=60s and
starting worker2 at t=120s. Captures exact event timestamps and per-second metrics.

Outputs:
- benchmarks/results/exp5_failure_timeseries_<ts>.csv (+ exp5_failure_timeseries_latest.csv)
- benchmarks/results/exp5_failure_events_<ts>.csv (+ exp5_failure_events_latest.csv)
- benchmarks/results/exp5_failure_requests_<ts>.csv
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h


def run_experiment(
    duration_sec: int = 180,
    stop_at: int = 60,
    start_at: int = 120,
    readers: int = 4,
    write_interval: float = 2.0,
    read_size_str: str = "4MB",
    write_size_str: str = "1MB",
    seed_objects: int = 8,
    reset_cluster: bool = False,
) -> Path:
    if reset_cluster:
        h.fresh_cluster()
    else:
        h.ensure_cluster_healthy()

    ts = h.utc_stamp()
    read_size = h.parse_size(read_size_str)
    write_size = h.parse_size(write_size_str)

    read_payload = h.make_payload(read_size)
    read_sha = h.sha256_hex(read_payload)
    write_payload = h.make_payload(write_size)

    seed_ids: List[str] = []
    writer_ids: List[str] = []
    req_lock = threading.Lock()
    state_lock = threading.Lock()
    req_logs: List[Dict] = []
    poll_snaps: Dict[int, Dict] = {}

    events: Dict[str, Optional[float]] = {
        "t0_stop_worker2": None,
        "dead_detected": None,
        "grace_period_end_est": None,
        "repair_complete": None,
        "t_restart_worker2": None,
        "worker_rejoined": None,
    }

    with h.make_client(timeout=60.0) as admin_client:
        print(f"[Exp 5] Pre-seeding {seed_objects} x {read_size_str} objects for continuous readers...", flush=True)
        for i in range(seed_objects):
            s = h.timed_upload(admin_client, read_payload, filename=f"exp5_seed_{i}.bin")
            if not s.ok or not s.object_id:
                raise RuntimeError(f"Seed upload failed: {s.status_code} {s.error}")
            seed_ids.append(s.object_id)

        print(f"[Exp 5] Starting {duration_sec}s run (stop worker2@{stop_at}s, start@{start_at}s)...", flush=True)
        t_start = time.monotonic()
        stop_event = threading.Event()

        def _reader(rid: int) -> None:
            idx = rid
            with h.make_client(timeout=30.0, max_connections=4) as client:
                while not stop_event.is_set():
                    rel_start = time.monotonic() - t_start
                    if rel_start >= duration_sec:
                        break
                    oid = seed_ids[idx % len(seed_ids)]
                    idx += 1
                    sample = h.timed_download(client, oid, expected_sha256=read_sha, verify_sha256=True)
                    rel_done = time.monotonic() - t_start
                    sec_bucket = min(duration_sec - 1, max(0, int(rel_start)))
                    with req_lock:
                        req_logs.append({
                            "sec": sec_bucket,
                            "rel_start_s": round(rel_start, 3),
                            "rel_end_s": round(rel_done, 3),
                            "role": f"reader-{rid}",
                            "op": "download",
                            "latency_ms": round(sample.elapsed_sec * 1000.0, 3),
                            "status_code": sample.status_code,
                            "byte_count": sample.byte_count if sample.ok else 0,
                            "ok": int(sample.ok),
                            "error": sample.error or "",
                        })

        def _writer() -> None:
            w_idx = 0
            next_tick = t_start
            with h.make_client(timeout=30.0, max_connections=4) as client:
                while not stop_event.is_set():
                    now_m = time.monotonic()
                    rel = now_m - t_start
                    if rel >= duration_sec:
                        break
                    sample = h.timed_upload(client, write_payload, filename=f"exp5_w_{w_idx}.bin")
                    rel_done = time.monotonic() - t_start
                    sec_bucket = min(duration_sec - 1, max(0, int(rel)))
                    if sample.object_id:
                        writer_ids.append(sample.object_id)
                        # Bound periodic writer objects so disk stays steady while recent objects persist
                        while len(writer_ids) > 10:
                            old_id = writer_ids.pop(0)
                            h.delete_object(client, old_id)
                    with req_lock:
                        req_logs.append({
                            "sec": sec_bucket,
                            "rel_start_s": round(rel, 3),
                            "rel_end_s": round(rel_done, 3),
                            "role": "writer-0",
                            "op": "upload",
                            "latency_ms": round(sample.elapsed_sec * 1000.0, 3),
                            "status_code": sample.status_code,
                            "byte_count": sample.byte_count if sample.ok else 0,
                            "ok": int(sample.ok),
                            "error": sample.error or "",
                        })
                    w_idx += 1
                    next_tick += write_interval
                    sleep_s = next_tick - time.monotonic()
                    if sleep_s > 0:
                        stop_event.wait(sleep_s)

        def _cluster_monitor() -> None:
            saw_under_rep = False
            saw_worker2_alive_after_restart = False
            with h.make_client(timeout=5.0) as mon_client:
                while not stop_event.is_set():
                    rel = time.monotonic() - t_start
                    if rel >= duration_sec:
                        break
                    sec_bucket = min(duration_sec - 1, max(0, int(rel)))
                    try:
                        workers = h.get_workers(mon_client)
                        rep = h.get_replication_summary(mon_client)
                        w2 = next((w for w in workers if w["worker_id"] == "worker2"), None)
                        w2_status = w2["status"] if w2 else "MISSING"
                        under = int(rep.get("under_replicated", 0))
                        over = int(rep.get("over_replicated", 0))
                        unavail = int(rep.get("unavailable", 0))

                        with state_lock:
                            poll_snaps[sec_bucket] = {
                                "worker2_status": w2_status,
                                "under_replicated": under,
                                "over_replicated": over,
                                "unavailable": unavail,
                            }
                            if events["t0_stop_worker2"] is not None:
                                if events["dead_detected"] is None and w2_status == "DEAD":
                                    events["dead_detected"] = rel
                                    events["grace_period_end_est"] = rel + 30.0
                                if under > 0:
                                    saw_under_rep = True
                                if (
                                    events["dead_detected"] is not None
                                    and events["repair_complete"] is None
                                    and saw_under_rep
                                    and under == 0
                                    and unavail == 0
                                ):
                                    events["repair_complete"] = rel
                            if events["t_restart_worker2"] is not None:
                                if w2_status == "ALIVE":
                                    saw_worker2_alive_after_restart = True
                                if (
                                    saw_worker2_alive_after_restart
                                    and events["worker_rejoined"] is None
                                    and over == 0
                                    and under == 0
                                ):
                                    events["worker_rejoined"] = rel
                    except Exception:
                        pass
                    stop_event.wait(0.5)

        threads = [threading.Thread(target=_reader, args=(i,), daemon=True) for i in range(readers)]
        threads.append(threading.Thread(target=_writer, daemon=True))
        threads.append(threading.Thread(target=_cluster_monitor, daemon=True))
        for t in threads:
            t.start()

        # Schedule worker2 stop at t=stop_at
        wait_stop = t_start + stop_at - time.monotonic()
        if wait_stop > 0:
            time.sleep(wait_stop)
        t_stop_call = time.monotonic() - t_start
        h.stop_worker("worker2")
        t_stop_done = time.monotonic() - t_start
        with state_lock:
            events["t0_stop_worker2"] = t_stop_call
        print(f"  [t={t_stop_done:.2f}s] docker stop worker2 executed", flush=True)

        # Schedule worker2 start at t=start_at
        wait_restart = t_start + start_at - time.monotonic()
        if wait_restart > 0:
            time.sleep(wait_restart)
        t_start_call = time.monotonic() - t_start
        h.start_worker("worker2")
        t_start_done = time.monotonic() - t_start
        with state_lock:
            events["t_restart_worker2"] = t_start_call
        print(f"  [t={t_start_done:.2f}s] docker start worker2 executed", flush=True)

        # Wait remaining run time
        rem = t_start + duration_sec - time.monotonic()
        if rem > 0:
            time.sleep(rem)
        stop_event.set()
        for t in threads:
            t.join(timeout=15.0)

        # If worker2 just rejoined close to 180s, wait briefly for cluster cleanup outside loop
        h.ensure_cluster_healthy(timeout=60.0)
        h.cleanup_objects(admin_client, seed_ids + writer_ids)

    # Aggregate per-second timeseries (always keep failed requests in error_count!)
    by_sec: Dict[int, List[Dict]] = {s: [] for s in range(duration_sec)}
    for r in req_logs:
        by_sec[int(r["sec"])].append(r)

    ts_rows: List[Dict] = []
    last_snap = {"worker2_status": "ALIVE", "under_replicated": 0, "over_replicated": 0, "unavailable": 0}
    for sec in range(duration_sec):
        if sec in poll_snaps:
            last_snap = poll_snaps[sec]
        items = by_sec[sec]
        ok_items = [x for x in items if x["ok"] == 1]
        ok_lats_sec = [x["latency_ms"] / 1000.0 for x in ok_items]
        lat_stats = h.summarize(ok_lats_sec)
        ok_bytes = sum(x["byte_count"] for x in ok_items)
        read_bytes = sum(x["byte_count"] for x in ok_items if x["op"] == "download")
        write_bytes = sum(x["byte_count"] for x in ok_items if x["op"] == "upload")
        ts_rows.append({
            "sec": sec,
            "req_count": len(items),
            "ok_count": len(ok_items),
            "error_count": len(items) - len(ok_items),
            "throughput_mb_s": round(ok_bytes / h.MB, 4),
            "read_throughput_mb_s": round(read_bytes / h.MB, 4),
            "write_throughput_mb_s": round(write_bytes / h.MB, 4),
            "latency_p50_ms": round(lat_stats["median"] * 1000.0, 3),
            "latency_p95_ms": round(lat_stats["p95"] * 1000.0, 3),
            "worker2_status": last_snap["worker2_status"],
            "under_replicated": last_snap["under_replicated"],
            "over_replicated": last_snap["over_replicated"],
            "unavailable": last_snap["unavailable"],
        })

    event_rows = [
        {"event": k, "rel_sec": round(v, 3) if v is not None else ""}
        for k, v in events.items()
    ]
    params_str = (
        f"duration={duration_sec}s;stop_at={stop_at}s;start_at={start_at}s;"
        f"readers={readers};read={read_size_str};write={write_size_str}@{write_interval}s"
    )
    h.write_csv(
        h.RESULTS_DIR / f"exp5_failure_requests_{ts}.csv",
        req_logs,
        experiment="exp5_failure_requests",
        params=params_str,
    )
    h.write_csv(
        h.RESULTS_DIR / f"exp5_failure_events_{ts}.csv",
        event_rows,
        experiment="exp5_failure_events",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp5_failure_events_latest.csv",
    )
    out = h.write_csv(
        h.RESULTS_DIR / f"exp5_failure_timeseries_{ts}.csv",
        ts_rows,
        experiment="exp5_failure_timeseries",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp5_failure_timeseries_latest.csv",
    )
    print(f"[Exp 5] Events: {events}", flush=True)
    print(f"[Exp 5] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 5: 180s Failure Injection Timeseries")
    parser.add_argument("--duration", type=int, default=180)
    parser.add_argument("--stop-at", type=int, default=60)
    parser.add_argument("--start-at", type=int, default=120)
    parser.add_argument("--readers", type=int, default=4)
    parser.add_argument("--write-interval", type=float, default=2.0)
    parser.add_argument("--read-size", default="4MB")
    parser.add_argument("--write-size", default="1MB")
    parser.add_argument("--seed-objects", type=int, default=8)
    parser.add_argument("--fresh-cluster", action="store_true")
    args = parser.parse_args()
    run_experiment(
        duration_sec=args.duration,
        stop_at=args.stop_at,
        start_at=args.start_at,
        readers=args.readers,
        write_interval=args.write_interval,
        read_size_str=args.read_size,
        write_size_str=args.write_size,
        seed_objects=args.seed_objects,
        reset_cluster=args.fresh_cluster,
    )


if __name__ == "__main__":
    main()
