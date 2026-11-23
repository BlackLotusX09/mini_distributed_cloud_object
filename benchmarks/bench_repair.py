"""Experiments 6 & 8: Repair speed vs stored volume (100 MB - 2 GB), write amplification, & balance.

Outputs:
- benchmarks/results/exp6_8_repair_<ts>.csv (+ exp6_8_repair_latest.csv)
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness as h

DEFAULT_VOLUMES = ["100MB", "500MB", "1GB", "2GB"]


def _populate_volume(target_bytes: int, obj_size: int, concurrency: int = 4) -> Dict:
    """Upload incompressible objects totaling `target_bytes` using `obj_size` mid-sized objects."""
    full_count = target_bytes // obj_size
    remainder = target_bytes % obj_size
    sizes = [obj_size] * full_count + ([remainder] if remainder > 0 else [])

    # Pre-generate base random chunk once per unique size outside upload timing
    full_buf = h.make_payload(obj_size) if full_count > 0 else b""
    rem_buf = h.make_payload(remainder) if remainder > 0 else b""

    lock = threading.Lock()
    uploaded_ids: List[str] = []
    logical_bytes = 0
    errors = 0

    idx_iter = iter(enumerate(sizes))
    iter_lock = threading.Lock()

    def _worker() -> None:
        nonlocal logical_bytes, errors
        with h.make_client(timeout=120.0, max_connections=8) as client:
            while True:
                with iter_lock:
                    nxt = next(idx_iter, None)
                if nxt is None:
                    return
                idx, sz = nxt
                payload = full_buf if sz == obj_size else rem_buf
                sample = h.timed_upload(client, payload, filename=f"exp6_obj_{idx:05d}.bin")
                with lock:
                    if sample.ok and sample.object_id:
                        uploaded_ids.append(sample.object_id)
                        logical_bytes += sz
                    else:
                        errors += 1

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(_worker) for _ in range(concurrency)]
        for f in futs:
            f.result()

    return {
        "object_ids": uploaded_ids,
        "logical_bytes": logical_bytes,
        "upload_errors": errors,
    }


def _worker_held_bytes(port: int, worker_id: str) -> tuple[int, int]:
    """Query exact chunk count and byte volume on a live worker before stopping it."""
    with httpx.Client(timeout=30.0) as c:
        resp = c.get(f"http://localhost:{port}/internal/workers/{worker_id}/chunks")
        resp.raise_for_status()
        chunks = resp.json().get("chunks", [])
        return len(chunks), sum(int(ch["size"]) for ch in chunks)


def run_experiment(
    volumes: List[str],
    object_size_str: str = "8MB",
    victim: str = "worker2",
    reset_between_volumes: bool = True,
) -> Path:
    obj_size = h.parse_size(object_size_str)
    ts = h.utc_stamp()
    rows: List[Dict] = []

    for idx, vol_label in enumerate(volumes):
        target_bytes = h.parse_size(vol_label)
        print(f"\n[Exp 6 & 8] Volume {vol_label} ({target_bytes} B) using {object_size_str} objects...",
              flush=True)

        if reset_between_volumes or idx == 0:
            h.fresh_cluster()
        else:
            h.ensure_cluster_healthy()

        pop = _populate_volume(target_bytes, obj_size, concurrency=4)
        logical_bytes = pop["logical_bytes"]

        with h.make_client(timeout=30.0) as client:
            rep_before = h.get_replication_summary(client)
            du_before = h.cluster_du_bytes()
            w_counts_before = {w["worker_id"]: w["valid_replicas"] for w in rep_before["workers"]}

        raw_before = sum(du_before.values())
        write_amp_before = raw_before / logical_bytes if logical_bytes > 0 else 0.0
        min_c = min(w_counts_before.values())
        max_c = max(w_counts_before.values())
        chunk_skew = (max_c / min_c) if min_c > 0 else float("inf")

        victim_port = h.WORKER_PORTS[victim]
        victim_chunks, victim_bytes = _worker_held_bytes(victim_port, victim)

        print(
            f"  Populated {logical_bytes / h.MB:.1f} MB | raw du={raw_before / h.MB:.1f} MB "
            f"(WA={write_amp_before:.3f}x) | chunk skew max/min={max_c}/{min_c}={chunk_skew:.3f} | "
            f"{victim} holds {victim_chunks} chunks ({victim_bytes / h.MB:.2f} MB)",
            flush=True,
        )

        # ---- Failure & Repair Timing
        with h.make_client(timeout=15.0) as mon_client:
            t0_wall = time.time()
            t0_mono = time.monotonic()
            h.stop_worker(victim)

            t_dead_mono = None
            dead_since_wall = None
            deadline_dead = t0_mono + 60.0
            while time.monotonic() < deadline_dead:
                ws = {w["worker_id"]: w for w in h.get_workers(mon_client)}
                if ws.get(victim, {}).get("status") == "DEAD":
                    t_dead_mono = time.monotonic()
                    dead_since_wall = ws[victim].get("dead_since")
                    break
                time.sleep(0.25)
            if t_dead_mono is None:
                raise TimeoutError(f"{victim} was not marked DEAD within 60s")

            detection_latency_s = t_dead_mono - t0_mono
            if dead_since_wall is None:
                dead_since_wall = t_dead_mono

            # Grace period = REPAIR_GRACE_PERIOD (30.0s) after dead_since
            grace_end_wall = float(dead_since_wall) + 30.0
            grace_end_mono = t0_mono + (grace_end_wall - t0_wall)

            # Poll until under_replicated hits 0; give generous bound for large volumes
            # Rate limit is MAX_REPAIRS_PER_CYCLE=20 per REPAIR_INTERVAL=10s (~2 chunks/s)
            est_repair_sec = max(120.0, (victim_chunks / 20.0 + 4) * 14.0)
            repair_deadline = time.monotonic() + est_repair_sec + 45.0
            t_first_progress_mono = None
            t_repair_done_mono = None
            initial_under = max(1, victim_chunks)

            while time.monotonic() < repair_deadline:
                rep = h.get_replication_summary(mon_client)
                under = int(rep.get("under_replicated", 0))
                if under > 0:
                    initial_under = max(initial_under, under)
                if t_first_progress_mono is None and 0 <= under < victim_chunks and time.monotonic() >= grace_end_mono - 1.0:
                    t_first_progress_mono = time.monotonic()
                if time.monotonic() >= grace_end_mono and under == 0:
                    t_repair_done_mono = time.monotonic()
                    break
                time.sleep(1.0)

            if t_repair_done_mono is None:
                raise TimeoutError(f"Repair did not finish within deadline for {vol_label}")

            grace_to_complete_s = max(0.01, t_repair_done_mono - grace_end_mono)
            active_repair_s = max(0.01, t_repair_done_mono - (t_first_progress_mono or grace_end_mono))
            repair_mb = victim_bytes / h.MB
            repair_mb_s = repair_mb / grace_to_complete_s if grace_to_complete_s > 0 else 0.0
            active_repair_mb_s = repair_mb / active_repair_s if active_repair_s > 0 else 0.0

            print(
                f"  Repair: detect={detection_latency_s:.2f}s | grace->done={grace_to_complete_s:.2f}s "
                f"({repair_mb_s:.2f} MB/s) | active={active_repair_s:.2f}s ({active_repair_mb_s:.2f} MB/s)",
                flush=True,
            )

            # ---- Restart victim and verify rejoin + over-replication trim
            t_restart_mono = time.monotonic()
            h.start_worker(victim)
            h.wait_until(
                lambda: (
                    any(w["worker_id"] == victim and w["status"] == "ALIVE" for w in h.get_workers(mon_client))
                    and h.get_replication_summary(mon_client)["over_replicated"] == 0
                    and h.get_replication_summary(mon_client)["under_replicated"] == 0
                ),
                timeout=180.0,
                interval=0.5,
                description=f"{victim} rejoin & trim after {vol_label}",
            )
            rejoin_reconcile_s = time.monotonic() - t_restart_mono
            du_after = h.cluster_du_bytes()
            raw_after = sum(du_after.values())
            write_amp_after = raw_after / logical_bytes if logical_bytes > 0 else 0.0

        rows.append({
            "volume_label": vol_label,
            "target_bytes": target_bytes,
            "object_size_bytes": obj_size,
            "logical_bytes": logical_bytes,
            "upload_errors": pop["upload_errors"],
            "raw_du_before_bytes": raw_before,
            "raw_du_after_bytes": raw_after,
            "write_amplification": round(write_amp_before, 4),
            "write_amplification_after_rejoin": round(write_amp_after, 4),
            "min_worker_chunks": min_c,
            "max_worker_chunks": max_c,
            "chunk_skew_max_min": round(chunk_skew, 4),
            "worker1_chunks": w_counts_before.get("worker1", 0),
            "worker2_chunks": w_counts_before.get("worker2", 0),
            "worker3_chunks": w_counts_before.get("worker3", 0),
            "worker4_chunks": w_counts_before.get("worker4", 0),
            "victim_worker": victim,
            "victim_chunks_lost": victim_chunks,
            "victim_bytes_lost": victim_bytes,
            "detection_latency_s": round(detection_latency_s, 3),
            "repair_duration_grace_to_done_s": round(grace_to_complete_s, 3),
            "repair_duration_active_s": round(active_repair_s, 3),
            "repair_mb_s": round(repair_mb_s, 4),
            "active_repair_mb_s": round(active_repair_mb_s, 4),
            "rejoin_reconcile_s": round(rejoin_reconcile_s, 3),
        })

    # Leave cluster clean on final volume exit
    h.cleanup_objects()
    params_str = f"volumes={','.join(volumes)};object_size={object_size_str};victim={victim}"
    out = h.write_csv(
        h.RESULTS_DIR / f"exp6_8_repair_{ts}.csv",
        rows,
        experiment="exp6_8_repair_and_balance",
        params=params_str,
        latest_path=h.RESULTS_DIR / "exp6_8_repair_latest.csv",
    )
    print(f"[Exp 6 & 8] Wrote {out}", flush=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp 6 & 8: Repair Speed, Write Amplification & Chunk Balance")
    parser.add_argument("--volumes", default=",".join(DEFAULT_VOLUMES))
    parser.add_argument("--object-size", default="8MB")
    parser.add_argument("--victim", default="worker2")
    parser.add_argument("--no-reset", action="store_true", help="Do not fresh_cluster() between volumes")
    args = parser.parse_args()
    vols = [v.strip() for v in args.volumes.split(",") if v.strip()]
    run_experiment(vols, object_size_str=args.object_size, victim=args.victim,
                   reset_between_volumes=not args.no_reset)


if __name__ == "__main__":
    main()
