# Two-Person Ownership & Evaluation Split (`cloud_project`)

This document divides the **Mini Distributed Cloud Object Store** cleanly and equitably between **Person A (Data Plane, Client API & Storage Engine)** and **Person B (Control Plane, Fault Tolerance & Self-Healing)** along the internal controller–worker and SQLite seam for final integration, live demonstration, and technical viva/evaluation.

---

## 1. High-Level Ownership Matrix

| Dimension | Person A — Data Plane, Client API & Storage Engine | Person B — Control Plane, Fault Tolerance & Self-Healing |
| --- | --- | --- |
| **Core Architectural Domain** | Client REST object lifecycle (`POST`, `GET`, `PATCH`, `DELETE`, `LIST`), 1 MiB streaming chunking & reconstruction, two-phase delete & startup rollback janitor, pooled `WorkerClient`, CLI object subcommands (`upload`, `download`, `info`, `patch`, `list`, `delete`), and the Worker HTTP daemon + crash-safe sharded disk storage engine. | SQLite WAL metadata schema (`workers`, `objects`, `chunks`, `replicas`, `custom_metadata`, `scrub_status`), worker registration & heartbeat liveness (`ALIVE`/`DEAD`), least-loaded distinct placement (`R=2`), grace-period background re-replication, worker rejoin inventory reconciliation, background/manual two-layer scrubber, and admin observability endpoints/CLI (`workers`, `chunks`, `replication`, `scrub`). |
| **Owned Controller Files** | [controller/objects_api.py](../controller/objects_api.py)<br>[controller/ingestion.py](../controller/ingestion.py)<br>[controller/replication.py](../controller/replication.py)<br>[controller/lifecycle.py](../controller/lifecycle.py)<br>[controller/validation.py](../controller/validation.py)<br>[controller/worker_client.py](../controller/worker_client.py)<br>[controller/errors.py](../controller/errors.py) | [controller/main.py](../controller/main.py)<br>[controller/db.py](../controller/db.py)<br>[controller/settings.py](../controller/settings.py)<br>[controller/placement.py](../controller/placement.py)<br>[controller/repair.py](../controller/repair.py)<br>[controller/reconciliation.py](../controller/reconciliation.py)<br>[controller/scrubber.py](../controller/scrubber.py)<br>[controller/obs.py](../controller/obs.py)<br>[controller/Dockerfile](../controller/Dockerfile)<br>[controller/requirements.txt](../controller/requirements.txt) |
| **Owned Worker & Client Files** | [worker/app.py](../worker/app.py)<br>[worker/storage/engine.py](../worker/storage/engine.py)<br>[worker/storage/__init__.py](../worker/storage/__init__.py)<br>[worker/config.py](../worker/config.py)<br>[worker/worker_obs.py](../worker/worker_obs.py)<br>[worker/Dockerfile](../worker/Dockerfile)<br>[client/objstore.py](../client/objstore.py) (`upload`, `download`, `info`, `patch`, `list`, `delete`)<br>[client/client.py](../client/client.py) | [worker/heartbeat.py](../worker/heartbeat.py)<br>[client/objstore.py](../client/objstore.py) (`workers`, `chunks`, `replication`, `scrub`)<br>[docker-compose.yml](../docker-compose.yml)<br>[pytest.ini](../pytest.ini)<br>[requirements-dev.txt](../requirements-dev.txt) |
| **Owned Scripts & Docs** | [scripts/demo_1_storage.sh](../scripts/demo_1_storage.sh)<br>[scripts/demo_2_chunking.sh](../scripts/demo_2_chunking.sh)<br>[scripts/demo_4_metadata.sh](../scripts/demo_4_metadata.sh)<br>[scripts/ensure_demo_files.py](../scripts/ensure_demo_files.py)<br>[scripts/local_smoke.sh](../scripts/local_smoke.sh)<br>**Benchmarks (Exp 1–4, Data Plane):**<br>[benchmarks/bench_throughput.py](../benchmarks/bench_throughput.py)<br>[benchmarks/bench_latency.py](../benchmarks/bench_latency.py)<br>[benchmarks/bench_concurrency.py](../benchmarks/bench_concurrency.py)<br>[benchmarks/bench_overhead.py](../benchmarks/bench_overhead.py)<br>[docs/diagrams/upload_sequence.mmd](diagrams/upload_sequence.mmd)<br>[README.md](../README.md) (REST API, CLI & Exp 1–4 results) | [scripts/demo_3_replication.sh](../scripts/demo_3_replication.sh)<br>[scripts/demo_5_failure.sh](../scripts/demo_5_failure.sh)<br>[scripts/demo_6_integrity.sh](../scripts/demo_6_integrity.sh)<br>[scripts/demo_integrity.sh](../scripts/demo_integrity.sh)<br>[scripts/corrupt_chunk.sh](../scripts/corrupt_chunk.sh)<br>[scripts/check_invariants.py](../scripts/check_invariants.py)<br>[scripts/check_health.sh](../scripts/check_health.sh)<br>[scripts/docker_verify.sh](../scripts/docker_verify.sh)<br>[scripts/print_replication.py](../scripts/print_replication.py)<br>[scripts/run_all_demos.sh](../scripts/run_all_demos.sh)<br>[scripts/lib.sh](../scripts/lib.sh)<br>**Benchmarks (Exp 5–8 & Harness, Control Plane):**<br>[benchmarks/harness.py](../benchmarks/harness.py)<br>[benchmarks/bench_failure.py](../benchmarks/bench_failure.py)<br>[benchmarks/bench_repair.py](../benchmarks/bench_repair.py)<br>[benchmarks/bench_scrub.py](../benchmarks/bench_scrub.py)<br>[benchmarks/plot.py](../benchmarks/plot.py)<br>[benchmarks/README.md](../benchmarks/README.md)<br>[docs/diagrams/architecture.mmd](diagrams/architecture.mmd)<br>[docs/diagrams/failure_timeline.mmd](diagrams/failure_timeline.mmd)<br>[docs/diagrams/replica_states.mmd](diagrams/replica_states.mmd)<br>[docs/REPORT.md](REPORT.md), [docs/TESTING.md](TESTING.md), [docs/design.md](design.md) |
| **Owned Public & Internal HTTP Endpoints** | **Public Client API (Controller `:8000`):**<br>• `POST /api/v1/objects` (`201`)<br>• `GET /api/v1/objects/{object_id}` (`200`/`404`/`500`/`503`)<br>• `GET /api/v1/objects/{object_id}/metadata` (`200`/`404`)<br>• `PATCH /api/v1/objects/{object_id}/metadata` (`200`/`400`/`404`)<br>• `DELETE /api/v1/objects/{object_id}` (`204`/`404`/`409`)<br>• `GET /api/v1/objects` (`200`/`400`)<br>**Internal Worker Data Plane (`:8001–:8004`):**<br>• `PUT /internal/chunks/{chunk_id}` (`201`/`200`/`400`/`409`)<br>• `GET /internal/chunks/{chunk_id}` (`200`/`404`/`409`)<br>• `DELETE /internal/chunks/{chunk_id}` (`204`)<br>• `GET /health` (worker disk & chunk count) | **Internal Control Plane (Controller `:8000`):**<br>• `POST /internal/workers/register` (`200`/`400`)<br>• `POST /internal/workers/heartbeat` (`200`/`404`)<br>• `GET /health` (controller & `active_workers`)<br>**Admin Observability & Control (`:8000`):**<br>• `GET /api/v1/admin/workers`<br>• `GET /api/v1/admin/objects/{object_id}/chunks`<br>• `GET /api/v1/admin/replication`<br>• `POST /api/v1/admin/scrub`<br>• `GET /api/v1/admin/placement-test`<br>**Worker Inventory & Scrub (`:8001–:8004`):**<br>• `GET /internal/workers/{worker_id}/chunks`<br>• `POST /internal/scrub` |
| **Owned Unit & Integration Test Files** | **Unit (`tests/unit/`):**<br>• [test_upload_download.py](../tests/unit/test_upload_download.py)<br>• [test_lifecycle.py](../tests/unit/test_lifecycle.py)<br>• [test_replication.py](../tests/unit/test_replication.py)<br>• [test_worker_client.py](../tests/unit/test_worker_client.py)<br>• [test_storage.py](../tests/unit/test_storage.py)<br>• [test_worker_http.py](../tests/unit/test_worker_http.py)<br>• [test_cli.py](../tests/unit/test_cli.py)<br>**Integration (`tests/integration/`):**<br>• [test_endpoints.py](../tests/integration/test_endpoints.py) | **Unit (`tests/unit/`):**<br>• [test_db.py](../tests/unit/test_db.py)<br>• [test_placement.py](../tests/unit/test_placement.py)<br>• [test_api.py](../tests/unit/test_api.py)<br>• [test_heartbeat.py](../tests/unit/test_heartbeat.py)<br>• [test_repair.py](../tests/unit/test_repair.py)<br>• [test_reconciliation.py](../tests/unit/test_reconciliation.py)<br>• [test_scrubber.py](../tests/unit/test_scrubber.py)<br>• [test_hardening.py](../tests/unit/test_hardening.py)<br>**Integration (`tests/integration/`):**<br>• [test_fault_tolerance.py](../tests/integration/test_fault_tolerance.py)<br>• [test_stress_and_restarts.py](../tests/integration/test_stress_and_restarts.py)<br>• [tests/conftest.py](../tests/conftest.py), [backends.py](../tests/integration/backends.py), [conftest.py](../tests/integration/conftest.py), [helpers.py](../tests/integration/helpers.py) |

---

## 2. Shared Interface Contract (The Seam Between Person A & Person B)

### 2.1 SQLite Tables & State Machine Seam (`controller/db.py`)

Person B owns the SQLite WAL schema and transaction helpers (`BEGIN IMMEDIATE`); Person A calls Person B's `db.*` functions and `placement.choose_workers()` without writing raw SQL in the data plane:

1. **`objects.status` Lifecycle (`PENDING` -> `COMMITTED` -> `DELETING`):**
   - **Upload start (Person A -> Person B DB):** `ingest_upload()` (`controller/ingestion.py`) calls `db.create_object_with_metadata(...)`, inserting `status='PENDING'`.
   - **Per-chunk commit (Person A -> Person B Placement & DB):** For each 1 MiB stripe, Person A calls `db.list_alive_with_counts()`, passes candidates into Person B's `placement.choose_workers(alive, n=2)` via `store_chunk()` (`controller/replication.py`), and records the 2 acknowledged replicas via `db.persist_chunk(chunk_id, object_id, idx, size, checksum, workers)` (`replicas.state='OK'`).
   - **Final commit (Person A -> Person B DB):** After EOF, `db.finalize_object(object_id, total_size, whole_sha256, chunk_count, now)` atomically flips `status='COMMITTED'`. While `PENDING`, Person B's repair loop ignores the object (`info["object_status"] != "COMMITTED"` in `controller/repair.py`) and Person B's rejoin reconciler preserves its chunks (`preserved_pending` in `controller/reconciliation.py`).
   - **Two-Phase Delete (Person A -> Person B Reconciler):** `delete_object()` (`controller/objects_api.py`) calls `db.begin_delete()`, setting `objects.status='DELETING'` and all replicas to `state='PENDING_DELETE'`. Confirmed worker `DELETE`s remove the replica rows (`db.apply_replica_cleanup()`, `db.finish_delete_if_empty()`); if a worker was `DEAD`/unreachable, the remaining `PENDING_DELETE` rows stay in SQLite until Person B's `reconcile_worker()` deletes them when the worker rejoins (`controller/reconciliation.py`).

2. **`replicas.state` Transitions (`OK`, `CORRUPT`, `PENDING_DELETE`):**
   - **Valid Replica Invariant:** A replica is valid if and only if `replicas.state == 'OK'` AND `workers.status == 'ALIVE'`.
   - **Read-Path Corruption Detection (Person A -> Person B Repair):** When Person A's `fetch_verified_chunk()` (`controller/objects_api.py`) receives `WorkerRejected` (`404`/`409`) or a controller-side SHA-256 mismatch, it calls `db.set_replica_state(chunk_id, worker_id, "CORRUPT")`, immediately wakes Person B's repair loop via `trigger_repair_event.set()`, and fails over to the next replica.
   - **Repair & Trim (Person B -> Person A WorkerClient):** Person B's `copy_chunk()` (`controller/repair.py`) reads from an `OK`+`ALIVE` source via `client.get_chunk()`, verifies SHA-256 against `chunks.checksum`, writes to a new distinct `ALIVE` target via `client.put_chunk()`, calls `db.upsert_replica_ok()`, and cleans up `CORRUPT` replicas (`client.delete_chunk()` or marks `PENDING_DELETE` if unreachable).

### 2.2 Internal HTTP Wire Contract (`controller/worker_client.py` <-> `worker/app.py`)

| Call Direction | Method & Path | Timeout (`settings.py`) | Request / Response Contract |
| --- | --- | --- | --- |
| **Worker -> Controller** (Person B -> Person B) | `POST /internal/workers/register` | `3.0s` (`HeartbeatDaemon`) | Body: `{"worker_id", "address", "free_bytes", "chunk_count"}` -> `200 {"status": "registered", "heartbeat_interval": 5}`. Schedules async `reconcile_worker()` if returning worker. |
| **Worker -> Controller** (Person B -> Person B) | `POST /internal/workers/heartbeat` | `3.0s` (`HeartbeatDaemon`) | Body: `{"worker_id", "timestamp", "free_bytes", "chunk_count"}` -> `200 {"status": "ok"}` or `404 UNKNOWN_WORKER` (triggers worker re-registration). Schedules `reconcile_worker()` on `DEAD -> ALIVE`. |
| **Controller -> Worker** (Person A & B -> Person A) | `PUT /internal/chunks/{chunk_id}` | `UPLOAD_WORKER_TIMEOUT = 10.0s` | Headers: `Content-Length`, `X-Chunk-Checksum`, `X-Request-ID`; raw binary body (`<= 1,048,576` B) -> `201 Created` (new) or `200 OK` (idempotent same content), `400 CHECKSUM_MISMATCH`/`CHUNK_TOO_LARGE`/`INVALID_CHUNK_ID`, `409 CHUNK_CONFLICT`. |
| **Controller -> Worker** (Person A & B -> Person A) | `GET /internal/chunks/{chunk_id}` | `WORKER_READ_TIMEOUT = 10.0s` | Verifies `<id>.chunk` against `<id>.sha256` sidecar -> `200 OK` (`application/octet-stream`, `X-Chunk-Checksum`) or `404 CHUNK_NOT_FOUND`, `409 CHUNK_CORRUPT` (never streams unverified bytes). |
| **Controller -> Worker** (Person A & B -> Person A) | `DELETE /internal/chunks/{chunk_id}` | `WORKER_DELETE_TIMEOUT = 5.0s` | Removes `<id>.chunk` and `<id>.sha256` -> `204 No Content` (idempotent; `WorkerClient.delete_chunk` treats `200`/`204`/`404` as success). |
| **Controller -> Worker** (Person B -> Person A) | `GET /internal/workers/{worker_id}/chunks` | `WORKER_READ_TIMEOUT = 10.0s` | Returns `200 {"worker_id", "chunks": [{"chunk_id", "size", "checksum"}], "chunk_mtimes": {chunk_id: mtime}}` via `storage.list_chunks()` and `storage.chunk_mtimes()`. |
| **Controller -> Worker** (Person B -> Person A) | `POST /internal/scrub` | `WORKER_SCRUB_TIMEOUT = 60.0s` | Streams every `.chunk` on disk in 64 KiB blocks (`yield_delay=0.005s`) -> `200 {"worker_id", "checked", "ok", "corrupt": [...], "missing_checksum": [...], "checksums": {cid: sha256}}`. |

### 2.3 Async Coordination Signals (`controller/main.py`)

- **`app.state.trigger_repair_event` (`asyncio.Event`):** Set by Person A's `fetch_verified_chunk()` on read corruption/rejection and by Person B's `scrubber.py` / `reconciliation.py` when corruption or under-replication is found, immediately waking `repair_loop()` without waiting for the 10s `REPAIR_INTERVAL` timer.
- **`app.state.repair_lock`, `app.state.scrub_lock`, `app.state.reconcile_lock` (`asyncio.Lock`):** Single-flight locks ensuring manual `POST /api/v1/admin/scrub`, periodic loops, and rejoin tasks never overlap or race.
- **`request_id_var` (`contextvars.ContextVar` in `controller/obs.py`):** Set per HTTP request by `RequestIdMiddleware` and forwarded automatically by Person A's `WorkerClient` via the `X-Request-ID` header into worker JSON-lines logs.

---

## 3. Remaining Work & Verification Checklist (Split by Person)

### Person A Checklist (Data Plane, Client API & Storage Engine)

- [ ] **Code Walkthrough & Audit:**
  - Verify streaming upload pipeline (`controller/objects_api.py`, `controller/ingestion.py`, `controller/replication.py`): pre-flight `len(alive) < 2` -> `503 INSUFFICIENT_WORKERS` with zero DB rows; 1 MiB chunk loop (`read_chunk`), distinct-worker retries (`MAX_RETRIES_PER_CHUNK=3`), rollback via `asyncio.shield(purge_object(...))` on mid-stream failure or client disconnect.
  - Verify streaming download pipeline (`controller/objects_api.py`): pre-flight Chunk `0` verification before sending HTTP `200` headers; replica fallback (`ALIVE` shuffled first, `DEAD` last resort); whole-object SHA-256 verification across streamed chunks.
  - Verify atomic disk engine (`worker/storage/engine.py`): 2-char prefix sharding (`$DATA_DIR/chunks/<prefix>/<id>.chunk` + `.sha256`), `.tmp` write + `fsync` + `os.replace` + `_fsync_dir`, startup orphan `.tmp` purge (`recovered_tmp_files`), and 64-stripe locking (`_LOCK_STRIPES=64`).
- [ ] **Unit & Integration Test Verification:**
  ```bash
  .venv/bin/python -m pytest \
    tests/unit/test_storage.py \
    tests/unit/test_worker_http.py \
    tests/unit/test_worker_client.py \
    tests/unit/test_replication.py \
    tests/unit/test_upload_download.py \
    tests/unit/test_lifecycle.py \
    tests/unit/test_cli.py \
    tests/integration/test_endpoints.py -v
  ```
- [ ] **Demo & Benchmark Rehearsal (Demos 1, 2, 4 & Exp 1–4):**
  ```bash
  bash scripts/demo_1_storage.sh --non-interactive
  bash scripts/demo_2_chunking.sh --non-interactive
  bash scripts/demo_4_metadata.sh --non-interactive
  .venv/bin/python benchmarks/bench_latency.py
  .venv/bin/python benchmarks/bench_overhead.py
  .venv/bin/python benchmarks/bench_concurrency.py
  .venv/bin/python benchmarks/bench_throughput.py
  ```

### Person B Checklist (Control Plane, Fault Tolerance & Self-Healing)

- [ ] **Code Walkthrough & Audit:**
  - Verify SQLite concurrency & schema (`controller/db.py`): `PRAGMA foreign_keys = ON`, `journal_mode = WAL`, `busy_timeout = 5000`, `BEGIN IMMEDIATE` transactions, and `ON DELETE CASCADE` on `chunks`, `replicas`, and `custom_metadata`.
  - Verify liveness & placement (`controller/main.py`, `controller/placement.py`): startup grace period (`HEARTBEAT_TIMEOUT=15.0s`), `sweep_dead_workers`, and `choose_workers` (fewest `OK` replicas first, random tie-break, `exclude` set).
  - Verify self-healing & scrubbing (`controller/repair.py`, `controller/reconciliation.py`, `controller/scrubber.py`): `REPAIR_GRACE_PERIOD=30.0s` for `DEAD`-only under-replication vs immediate repair for `CORRUPT` replicas, `db_reps_snapshot` captured before `list_worker_chunks()` on rejoin, `trim_over_replicated()`, and two-layer scrub checksum comparison.
- [ ] **Unit & Integration Test Verification:**
  ```bash
  .venv/bin/python -m pytest \
    tests/unit/test_db.py \
    tests/unit/test_placement.py \
    tests/unit/test_api.py \
    tests/unit/test_heartbeat.py \
    tests/unit/test_repair.py \
    tests/unit/test_reconciliation.py \
    tests/unit/test_scrubber.py \
    tests/unit/test_hardening.py \
    tests/integration/test_fault_tolerance.py \
    tests/integration/test_stress_and_restarts.py -v
  ```
- [ ] **Demo, Benchmark & Invariant Rehearsal (Demos 3, 5, 6, Exp 5–8 & Invariants):**
  ```bash
  bash scripts/demo_3_replication.sh --non-interactive
  bash scripts/demo_5_failure.sh --non-interactive
  bash scripts/demo_6_integrity.sh --non-interactive --include-unrecoverable
  .venv/bin/python benchmarks/bench_failure.py
  .venv/bin/python benchmarks/bench_repair.py
  .venv/bin/python benchmarks/bench_scrub.py
  .venv/bin/python benchmarks/plot.py
  .venv/bin/python scripts/check_invariants.py
  ```

---

## 4. Live Demo Execution Script (Alternating 50/50 Split)

Before starting the live evaluation, verify the cluster is up:
```bash
docker compose up -d --build
bash scripts/check_health.sh
python3 client/objstore.py workers
```

### Demo 1 — Object Storage (Text & Binary Round-Trip) -> **Person A**
1. **Operator:** Person A
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_1_storage.sh
   # Or step-by-step via CLI:
   python3 client/objstore.py upload demo/files/sample.txt --meta purpose=demo1 --meta kind=text
   python3 client/objstore.py upload demo/files/sample.png --meta purpose=demo1 --meta kind=image
   python3 client/objstore.py info <OBJECT_ID>
   python3 client/objstore.py download <OBJECT_ID> -o /tmp/dl_sample.png
   cmp -s demo/files/sample.png /tmp/dl_sample.png && echo "BYTE-EXACT MATCH"
   ```
3. **Key Talking Points (Person A):**
   - Uploads enter SQLite as `PENDING` (`controller/ingestion.py`) and only become visible (`COMMITTED`) after all chunks are replicated to 2 workers.
   - Both text (`text/plain`) and binary (`image/png`) preserve original MIME types, byte lengths, custom metadata, and end-to-end SHA-256 checksums (`VERIFIED` by the CLI on download).

### Demo 2 — Chunking (1 MiB Stripes) & Worker Distribution -> **Person A**
1. **Operator:** Person A
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_2_chunking.sh
   # Or inspect directly:
   python3 client/objstore.py chunks <MEDIUM_6MB_OBJECT_ID>
   python3 client/objstore.py workers
   ```
3. **Key Talking Points (Person A):**
   - `demo/files/medium_6mb.bin` (`6,500,000` bytes) is streamed strictly one `1,048,576`-byte (`1 MiB`) chunk at a time (`read_chunk()` in `controller/ingestion.py`), producing 6 full chunks (`000000`..`000005` of `1,048,576` B) and 1 tail chunk (`000006` of `208,544` B).
   - Constant memory footprint: the controller deletes each 1 MiB buffer (`del data`) immediately after replicating it, verified up to 250 MiB in `tests/integration/test_stress_and_restarts.py`.

### Demo 3 — Replication Factor 2 & Disk-to-DB Parity -> **Person B**
1. **Operator:** Person B
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_3_replication.sh
   # And show live worker container disk contents:
   docker exec worker1 find /data/chunks -type f | head -n 10
   ```
3. **Key Talking Points (Person B):**
   - `choose_workers()` (`controller/placement.py`) ranks `ALIVE` workers by lowest active `OK` chunk count with random tie-breaking, guaranteeing 2 distinct workers per chunk and even balance across `worker1`–`worker4`.
   - Shows 1:1 parity between SQLite `replicas` and the physical `<chunk_id>.chunk` + `<chunk_id>.sha256` files inside each worker container's `/data/chunks/<2-char-prefix>/` volume.

### Demo 4 — Metadata Operations (Zero Worker Data Traffic on PATCH) -> **Person A**
1. **Operator:** Person A
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_4_metadata.sh
   # Or via CLI:
   python3 client/objstore.py info <OBJECT_ID>
   python3 client/objstore.py patch <OBJECT_ID> env=prod tier=gold owner=null
   python3 client/objstore.py info <OBJECT_ID>
   ```
3. **Key Talking Points (Person A):**
   - `GET /api/v1/objects/{id}/metadata` and `PATCH /api/v1/objects/{id}/metadata` (`controller/objects_api.py`) operate purely on SQLite (`objects` + `custom_metadata`) without contacting any worker node (proven even when all 4 workers are stopped in `test_metadata_get_works_with_all_workers_stopped`).
   - Merge-patch semantics: updates existing keys (`env=prod`), adds new keys (`tier=gold`), and deletes keys set to `null`/empty (`owner=null`), advancing `modified_at` while `created_at`, `checksum`, chunk map, and worker disk `mtime`s remain untouched (`0` worker `PUT` requests).

### Demo 5 — Worker Failure, Re-Replication & Rejoin -> **Person B**
1. **Operator:** Person B
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_5_failure.sh
   # Executes the 4 lifecycle stages around:
   #   docker stop worker2 -> download during outage -> wait past 30s grace period -> docker start worker2
   ```
3. **Key Talking Points (Person B):**
   - **Detection & Read Continuity:** Stopping `worker2` causes heartbeats (`5s` interval) to cease; after `HEARTBEAT_TIMEOUT = 15s`, `sweep_dead_workers()` marks `worker2` `DEAD` with `dead_since`. Client downloads immediately succeed by routing to the surviving `ALIVE` replica.
   - **Grace-Period Repair:** The repair loop waits `REPAIR_GRACE_PERIOD = 30s` from `dead_since` so transient restarts do not cause unnecessary network copies, then re-replicates under-replicated chunks from surviving `ALIVE` sources onto `worker1`/`worker3`/`worker4` to restore `R = 2`.
   - **Rejoin Reconciliation & Trimming:** When `docker start worker2` runs, `worker2` re-registers (`DEAD -> ALIVE`), triggering `reconcile_worker()` (`controller/reconciliation.py`) and `trim_over_replicated()`, which deletes excess 3rd copies and brings `over_replicated` back to `0`.

### Demo 6 — Two-Layer Data Integrity, Scrubbing & Self-Healing -> **Person B**
1. **Operator:** Person B
2. **Commands to Execute:**
   ```bash
   bash scripts/demo_6_integrity.sh --include-unrecoverable
   .venv/bin/python scripts/check_invariants.py
   ```
3. **Key Talking Points (Person B):**
   - **Single-Replica Bit-Rot:** `scripts/corrupt_chunk.sh` flips a byte in chunk `0` on one worker container's disk. Downloading the object immediately succeeds: the worker detects the `.sha256` sidecar mismatch and returns `409 CHUNK_CORRUPT` (or the controller detects a DB SHA-256 mismatch), marks that replica `CORRUPT`, wakes `repair_loop`, and streams the intact replica from the other worker.
   - **Scrub & Heal:** `python3 client/objstore.py scrub` (`POST /api/v1/admin/scrub`) scans all worker disks and cross-checks reported hashes against SQLite `chunks.checksum`. The repair loop copies a clean replica to a healthy distinct worker and deletes the corrupt file from the victim worker.
   - **Dual Corruption Fail-Stop:** When both replicas of chunk `0` are corrupted, pre-flight verification aborts cleanly with `HTTP 500 {"error": "DATA_CORRUPT", ...}` rather than streaming corrupt bytes.

---

## 5. Viva & Examiner Q&A Ownership Map

### Person A's Questions (Data Plane, Streaming, Error Contracts & Worker Storage)

1. **How do you guarantee constant memory usage when uploading or downloading a 250 MiB file?**
   - On upload, FastAPI spools multipart uploads above 1 MiB to disk, and `read_chunk()` (`controller/ingestion.py`) reads at most `CHUNK_SIZE = 1,048,576` bytes per iteration, replicates those bytes to 2 workers in parallel, persists metadata, and frees the buffer (`del data`).
   - On download, `download_object()` (`controller/objects_api.py`) pre-fetches only Chunk `0` (to return a clean `500`/`503` JSON error if Chunk `0` is unavailable) and streams chunks `1..N-1` one chunk at a time via FastAPI `StreamingResponse` (`stream_chunks()`).

2. **What happens if a worker crashes mid-upload, or the client disconnects halfway through an upload?**
   - For a single chunk, `store_chunk()` (`controller/replication.py`) keeps already-acknowledged replicas and retries only the failed replica on a distinct untried `ALIVE` worker up to `MAX_RETRIES_PER_CHUNK = 3` rounds.
   - If fewer than 2 workers can store the chunk (or if the client disconnects raising `CancelledError`), `ingest_upload()` (`controller/ingestion.py`) runs `await asyncio.shield(purge_object(client, object_id, inflight))` to delete all written and in-flight chunks from workers and clean up SQLite. On controller crash mid-upload, `_run_janitor()` cleans up `PENDING` objects older than `PENDING_OBJECT_TTL_SECONDS = 600s` at startup.

3. **How does the worker guarantee crash-safe, atomic chunk writes without partial files on `SIGKILL`?**
   - In `worker/storage/engine.py`, `write_chunk()` writes payload bytes to `.<id>.tmp.<thread_id>`, calls `os.fsync()`, atomically renames it to `<id>.chunk` via `os.replace()`, and only then writes and atomically renames `.<id>.sha256.tmp` to `<id>.sha256` followed by `_fsync_dir()`.
   - A chunk exists only if **both** `<id>.chunk` and a valid 64-hex-char `<id>.sha256` exist. On startup, `_recover()` removes any leftover `.tmp` files and orphan `.chunk` files missing their `.sha256` commit marker.

4. **Why are there two layers of SHA-256 checksum verification?**
   - **Layer 1 (Worker local):** The worker verifies `X-Chunk-Checksum` before committing a `PUT`, and re-hashes `<id>.chunk` against its on-disk `<id>.sha256` sidecar on every `GET` (returning `409 CHUNK_CORRUPT` on mismatch).
   - **Layer 2 (Controller authoritative):** The controller stores per-chunk SHA-256 in SQLite `chunks.checksum` and whole-object SHA-256 in `objects.checksum`. Even if both `<id>.chunk` and `<id>.sha256` on a worker were tampered with together, the controller re-verifies `hashlib.sha256(data).hexdigest() == chunk["checksum"]` on every read, repair copy, and scrub report.

5. **How does two-phase delete handle a worker that is down during `DELETE /api/v1/objects/{id}`?**
   - `db.begin_delete()` atomically flips the object to `DELETING` (making it immediately `404` to clients) and marks all its replica rows `PENDING_DELETE`.
   - `delete_replicas()` sends parallel `DELETE /internal/chunks/{id}` requests (`WORKER_DELETE_TIMEOUT = 5.0s`, concurrency `16`). Reachable replica rows are removed immediately; unreachable workers retain `PENDING_DELETE` rows in SQLite (with the parent object kept in `DELETING` state so foreign keys remain intact) until the worker rejoins and `reconcile_worker()` physically deletes the orphan files.

---

### Person B's Questions (Control Plane, Liveness, Placement, Repair, Reconciliation & Scrubbing)

1. **Why separate `HEARTBEAT_TIMEOUT` (`15s`) from `REPAIR_GRACE_PERIOD` (`30s`)?**
   - `HEARTBEAT_TIMEOUT = 15.0s` (workers heartbeat every `5s`; `LIVENESS_CHECK_INTERVAL = 3.0s`) quickly detects an unresponsive worker and marks it `DEAD` so new uploads stop placing chunks on it and reads try surviving `ALIVE` workers first.
   - Waiting `REPAIR_GRACE_PERIOD = 30.0s` after `dead_since` before re-replicating `DEAD`-worker chunks absorbs brief restarts without wasting bandwidth or causing `over_replicated` churn (verified by `test_fast_flap_creates_no_new_replicas`), whereas `CORRUPT` replicas bypass the grace period and repair immediately.

2. **How do you prevent false-positive `DEAD` transitions when the controller container itself restarts?**
   - In `controller/main.py`, `liveness_monitor()` records `started = time.monotonic()` at boot and skips `sweep_dead_workers()` until `time.monotonic() - started >= cfg.heartbeat_timeout` (`15s`), giving live workers time to send fresh heartbeats (`5s` interval) before liveness sweeps begin.

3. **Walk through all cases handled by `reconcile_worker()` when a dead worker rejoins the cluster.**
   - `reconcile_worker()` (`controller/reconciliation.py`) captures `db_reps_snapshot` **before** calling `GET /internal/workers/{id}/chunks` (avoiding races with concurrent uploads), then processes:
     1. **`PENDING_DELETE` replicas:** Deletes the chunk on the worker, deletes the DB replica row, and finalizes any `DELETING` parent object.
     2. **Active `PENDING` uploads:** Preserves on-disk chunks belonging to in-flight uploads (`preserved_pending`).
     3. **`DELETING` objects or corrupt disk checksums:** Deletes chunks belonging to `DELETING` objects; if a disk chunk's SHA-256 mismatches `chunks.checksum`, marks the replica `CORRUPT` and triggers repair.
     4. **Known valid replicas & unrecorded valid copies:** Restores `CORRUPT -> OK` if on-disk checksum matches DB; adopts unrecorded valid copies when `valid_count < 2`, or deletes redundant copies when `valid_count >= 2`.
     5. **Unknown orphan chunks (not in DB):** Preserves files younger than `ORPHAN_SAFETY_TTL_SECONDS = 600s` (protecting pre-commit in-flight chunks) and deletes expired orphans (`>= 600s`).
     6. **Stale DB rows (in DB snapshot, missing on disk):** Removes the stale DB `replicas` row and triggers repair if `valid_count < 2`.
     7. **Over-replication (`valid_count > 2`):** Calls `trim_over_replicated()` to remove excess copies starting from the highest-loaded worker.

4. **How does the background repair loop avoid starving client traffic or spinning forever on an unrepairable chunk?**
   - `controller/repair.py` caps each cycle at `MAX_REPAIRS_PER_CYCLE = 20` chunks, bounds parallel transfers via `asyncio.Semaphore(REPAIR_CONCURRENCY = 4)`, and tracks consecutive failures per `chunk_id` in `repair_attempts`, skipping chunks after `MAX_CHUNK_REPAIR_ATTEMPTS = 5` (circuit breaker) or when no valid source/target exists.

5. **What is the single point of failure in this architecture, and how is SQLite concurrency protected?**
   - The controller node and its local SQLite database (`/data/controller.db`) are a single point of failure (documented in `docs/REPORT.md`).
   - Within the controller, every thread opens an independent SQLite connection configured with `PRAGMA journal_mode = WAL`, `PRAGMA foreign_keys = ON`, and `PRAGMA busy_timeout = 5000`, and multi-step mutations use `BEGIN IMMEDIATE` transactions (`controller/db.py`) so concurrent uploads, parallel metadata `PATCH`es, heartbeats, and repair cycles serialize cleanly without `SQLITE_BUSY` errors or lost updates.
