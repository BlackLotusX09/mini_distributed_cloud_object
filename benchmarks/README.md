# Distributed Object Store Benchmarking Suite

Reproducible, isolated performance and fault-tolerance benchmarking harness for the Distributed Object Store (`controller` + 4 stateful chunk workers in Docker Compose).

---

## Methodology & Invariants

- **Strict Code Isolation:** Core `controller/` and `worker/` logic from tag `v1.0` was kept untouched; all measurements execute externally on the `bench` branch via HTTP/Docker CLI.
- **Byte & Throughput Units:** `1 KB = 1,024 B`, `1 MB = 1,048,576 B` (`1 MiB`), `1 GB = 1,073,741,824 B`.
- **Incompressible Payloads Outside Timers:** Every payload is pre-generated with `os.urandom()` outside `time.perf_counter()` sections (never zero-filled buffers that VM page deduplication or transparent compression could distort).
- **Clean Timer Boundaries:** `time.perf_counter()` wraps only the HTTP request/response transfer lifecycle in [harness.py](harness.py). JSON parsing, SHA-256 verification (`timed_download(..., verify_sha256=True)`), and object cleanup occur outside timed blocks.
- **No Dropped Errors:** Every failed HTTP status, timeout, socket exception, or checksum mismatch is recorded as a first-class row in raw CSVs and counted in `error_count` / `checksum_failures`.
- **Full Provenance:** Every row in every CSV records `timestamp`, `git_commit`, `experiment`, `params`, active DFS configuration (`CHUNK_SIZE`, `R`, timeouts, grace period, repair/scrub settings), and host/Docker Desktop VM specs.

---

## Test Environment & VM Limitations

- **Host Hardware:** Apple M4 (`10` CPU cores, `16.0 GiB` unified memory), macOS `15.7.9`.
- **Docker Runtime:** Docker Desktop `29.8.0` Linux VM (`10` vCPUs, `7.7 GiB` RAM, `453 GB` virtual disk).
- **VM & Single-Host Caveats:**
  1. All 5 containers share one Docker bridge network (`dfs-network`) inside a single Linux VM rather than physical NICs across 5 hosts; wire latencies reflect VM loopback switching rather than WAN/LAN RTT.
  2. Container volumes (`/data`) sit on the Docker Desktop Linux VM virtual block device, so high-concurrency writes and host buffer-cache pressure share one underlying SSD/VM disk queue.
  3. Controller multipart uploads above `1 MiB` spool to `/tmp` inside the controller container (`request.form()`) before 1 MiB chunking, causing large single-stream uploads (`256 MB`–`1 GB`) to incur extra container-disk write/read amplification.

---

## Running the Benchmarks & Plots

Ensure the Docker Compose stack is up (`docker compose up -d --build && bash scripts/check_health.sh`) and `.venv` has `httpx` and `matplotlib` installed:

| Exp | Script | Command | Typical Runtime | Output CSV(s) |
| --- | --- | --- | --- | --- |
| **1** | [bench_throughput.py](bench_throughput.py) | `.venv/bin/python benchmarks/bench_throughput.py` | ~6.5 min | `results/exp1_throughput_latest.csv` |
| **2** | [bench_latency.py](bench_latency.py) | `.venv/bin/python benchmarks/bench_latency.py` | ~30 s | `results/exp2_latency_latest.csv` |
| **3** | [bench_concurrency.py](bench_concurrency.py) | `.venv/bin/python benchmarks/bench_concurrency.py` | ~35 s | `results/exp3_concurrency_latest.csv` |
| **4** | [bench_overhead.py](bench_overhead.py) | `.venv/bin/python benchmarks/bench_overhead.py --fresh-cluster` | ~40 s | `results/exp4_overhead_latest.csv` |
| **5** | [bench_failure.py](bench_failure.py) | `.venv/bin/python benchmarks/bench_failure.py` | ~3 min | `results/exp5_failure_timeseries_latest.csv` |
| **6 & 8** | [bench_repair.py](bench_repair.py) | `.venv/bin/python benchmarks/bench_repair.py` | ~20 min | `results/exp6_8_repair_latest.csv` |
| **7** | [bench_scrub.py](bench_scrub.py) | `.venv/bin/python benchmarks/bench_scrub.py` | ~95 s | `results/exp7_scrub_latest.csv` |
| **Plots** | [plot.py](plot.py) | `.venv/bin/python benchmarks/plot.py` | ~5 s | `plots/exp*.png` (7 charts @ 300 DPI) |

Each script supports CLI flags (`--sizes`, `--runs`, `--warmup`, `--volumes`, `--per-worker-size`, `--concurrency`, `--fresh-cluster`) for quick smoke runs or custom sweeps.

---

## Summary of Measured Results (`v1.0` / `bench`)

| Experiment | Key Measured Finding | Output Plot |
| --- | --- | --- |
| **Exp 1: Throughput vs Size** (`1 KB`–`1 GB`, 10 runs each) | Upload peaks at **81.51 MB/s** (`64 MB`) before Starlette multipart spooling throttles `1 GB` to **53.12 MB/s**; Download peaks at **246.50 MB/s** (`256 MB`, **220.94 MB/s** at `1 GB`). **0 errors, 0 SHA-256 mismatches.** | [exp1_throughput_vs_size.png](plots/exp1_throughput_vs_size.png) |
| **Exp 2: Small-Object Latency** (500 seq reqs/size) | `1 KB`: Upload p50=`9.02 ms` / p99=`13.63 ms`, Download p50=`2.02 ms` / p99=`3.20 ms`. `100 KB`: Upload p50=`9.86 ms` / p99=`13.35 ms`, Download p50=`2.37 ms` / p99=`5.61 ms`. **0/3,000 errors.** | [exp2_small_object_latency.png](plots/exp2_small_object_latency.png) |
| **Exp 3: Concurrency Scaling** (`1`–`16` threads @ `16 MB`) | Aggregate Download scales from `166.34 MB/s` (`c=1`) to **244.42 MB/s** (`c=8`); Aggregate Upload scales from `71.22 MB/s` (`c=1`) to **149.54 MB/s** (`c=16`), where controller CPU reaches **126.6%** (`workers_sum=65.3%`). **0 errors.** | [exp3_concurrency_scaling.png](plots/exp3_concurrency_scaling.png) |
| **Exp 4: Controller Overhead** (`1 MB`, 50 runs) | Upload: Direct 1x=`7.97 ms`, Direct 2x=`15.95 ms`, Controller=`17.30 ms` (**+9.32 ms** vs 1x, only **+1.35 ms** vs 2x due to parallel fan-out). Download: Direct 1x=`4.85 ms`, Controller=`7.41 ms` (**+2.56 ms**). | [exp4_controller_overhead.png](plots/exp4_controller_overhead.png) |
| **Exp 5: Live Failure Timeline** (`180s`, `stop@60s`, `start@120s`) | Mean throughput **209.32 MB/s** with **0/9,487 client errors**. `worker2` detected `DEAD` at `76.26s` (`+16.3s`), repaired to `R=2` at `111.33s` (`+4.9s` post-30s grace), rejoined & trimmed at `121.23s`. | [exp5_failure_timeline.png](plots/exp5_failure_timeline.png) |
| **Exp 6 & 8: Repair Speed, WA & Skew** (`100 MB`–`2 GB`) | Repair scales linearly at **1.94–2.18 MB/s** (`23.0s` for `100 MB` up to `526.3s` for `2 GB`), matching the `MAX_REPAIRS_PER_CYCLE=20 / REPAIR_INTERVAL=10s` throttle. Write amplification **2.0001x** (ideal `2.0x`), worker chunk skew **`1.000–1.002x`**. | [exp6_8_repair_and_storage.png](plots/exp6_8_repair_and_storage.png) |
| **Exp 7: Cluster Scrubbing** (`1 GB`/worker, `4,096` chunks) | Idle scrub finishes `4,096 MB` in **35.35 s** (**115.87 MB/s**); under 4 concurrent readers (**578** x `16 MB` downloads = **247.2 MB/s**, **0 errors**), scrub completes in **37.41 s** (**109.48 MB/s**, only **5.8%** overhead). | [exp7_scrub_throughput.png](plots/exp7_scrub_throughput.png) |
