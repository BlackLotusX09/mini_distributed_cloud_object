# Technical Engineering Report: Distributed Object Storage System

## 1. Introduction

This report documents the architecture, implementation, failure recovery mechanisms, data integrity verification, observability, and empirical verification of our fault-tolerant Distributed Object Store. Designed around a coordinator-worker topology (`1` controller + `4` stateless-protocol / stateful-disk worker daemons running in Docker Compose), the system provides S3-like HTTP object storage with 1 MiB streaming chunking, replication factor $R = 2$ across distinct failure domains, automatic re-replication on node loss, and two-layer SHA-256 cryptographic integrity verification.

---

## 2. Architecture & Data Model

### System Topology

```mermaid
flowchart TB
    Client["Client / CLI (client/objstore.py) or curl"]
    Client -->|"HTTP REST API :8000 / X-Request-ID"| Controller

    subgraph DockerNet["Docker Network (dfs-network)"]
        Controller["Controller Node (FastAPI + uvicorn)\nChunking, Placement, Repair, Scrub, Liveness"]
        DB[("SQLite WAL\n/data/controller.db")]
        Controller <--> DB

        Controller <-->|"PUT/GET/DELETE /internal/chunks/{id}\nGET /internal/workers/{id}/chunks\nPOST /internal/scrub"| W1["worker1 :8001"]
        Controller <-->|"Internal HTTP"| W2["worker2 :8002"]
        Controller <-->|"Internal HTTP"| W3["worker3 :8003"]
        Controller <-->|"Internal HTTP"| W4["worker4 :8004"]

        W1 --> V1[("worker1-data\n/data/chunks")]
        W2 --> V2[("worker2-data\n/data/chunks")]
        W3 --> V3[("worker3-data\n/data/chunks")]
        W4 --> V4[("worker4-data\n/data/chunks")]
    end
```

### Metadata Schema (`controller/db.py`)

Metadata is stored in SQLite (`PRAGMA foreign_keys = ON; journal_mode = WAL; busy_timeout = 5000`) across 6 tables:
1. **`workers`**: `(worker_id PK, address, status IN ('ALIVE','DEAD'), last_heartbeat, dead_since, registered_at)`.
2. **`objects`**: `(object_id PK, name, size, content_type, checksum, chunk_count, status IN ('PENDING','COMMITTED','DELETING'), created_at, modified_at)`.
3. **`chunks`**: `(chunk_id PK, object_id FK ON DELETE CASCADE, idx, size, checksum)` with unique index `(object_id, idx)`. Chunk IDs follow `{object_id}_{idx:06d}`.
4. **`replicas`**: `(chunk_id FK ON DELETE CASCADE, worker_id FK, state IN ('OK','CORRUPT','PENDING_DELETE'))` with composite primary key `(chunk_id, worker_id)`.
5. **`custom_metadata`**: `(object_id FK ON DELETE CASCADE, key, value)` storing up to `64` string keys and `16,384` serialized bytes.
6. **`scrub_status`**: singleton row persisting `(last_scrub_at, chunks_checked, corrupt_found)`.

### Worker Storage Layout (`worker/storage/engine.py`)

Each worker stores chunks sharded by the first two characters of `chunk_id` under `$DATA_DIR/chunks/<prefix>/`:
- `<chunk_id>.chunk`: raw byte payload (up to `1,048,576` bytes).
- `<chunk_id>.sha256`: 64-character hex SHA-256 sidecar committed last via atomic write-temp + `fsync` + `os.replace` + directory `fsync`.

A chunk is considered committed on disk only when both `.chunk` and valid `.sha256` exist; orphan `.tmp` staging files from mid-write `SIGKILL` crashes are purged cleanly during startup recovery.

---

## 3. Core Read, Write & Delete Flows

### Streaming Upload & Atomic Commit (`controller/ingestion.py`)

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant Ctrl as Controller (FastAPI)
    participant DB as SQLite (WAL)
    participant W1 as Worker A (Alive)
    participant W2 as Worker B (Alive)
    participant W3 as Worker C (Alive Retry)

    C->>Ctrl: POST /api/v1/objects (multipart stream)
    Ctrl->>DB: list_alive_with_counts()
    alt fewer than R=2 alive workers
        Ctrl-->>C: 503 INSUFFICIENT_WORKERS (zero DB rows)
    else >= 2 alive workers
        Ctrl->>DB: INSERT objects (status=PENDING) + custom_metadata
        loop For each 1 MiB chunk idx = 0..N-1
            Ctrl->>Ctrl: Read up to 1 MiB + compute SHA-256
            Ctrl->>DB: choose_workers (fewest OK chunks, distinct)
            par Parallel chunk dispatch
                Ctrl->>W1: PUT /internal/chunks/{id} (X-Chunk-Checksum)
                Ctrl->>W2: PUT /internal/chunks/{id} (X-Chunk-Checksum)
            end
            alt Both ACK (200/201)
                Ctrl->>DB: BEGIN IMMEDIATE -> INSERT chunk + 2 OK replicas -> COMMIT
            else Partial failure on W2
                W2-->>Ctrl: Error / Timeout
                Ctrl->>W3: Retry missing replica (distinct untried worker)
                alt Retry succeeds
                    Ctrl->>DB: INSERT chunk + replicas (W1, W3)
                else Retries exhausted -> Rollback
                    Ctrl->>W1: Best-effort DELETE partial chunks
                    Ctrl->>W2: Best-effort DELETE partial chunks
                    Ctrl-->>C: 503 STORAGE_UNAVAILABLE
                end
            end
        end
        Ctrl->>DB: UPDATE objects SET status=COMMITTED, size, checksum, chunk_count
        Ctrl-->>C: 201 Created
    end
```

- **Constant Memory:** Controller reads at most `CHUNK_SIZE` (`1,048,576` B) per chunk via `read_chunk()` and discards each buffer immediately after `persist_chunk()`.
- **Visibility Isolation:** `PENDING` and `DELETING` objects return `404 OBJECT_NOT_FOUND` across `GET /objects/{id}`, `GET/PATCH /metadata`, and are excluded from `GET /objects`.

### Streaming Download & Fallback (`controller/objects_api.py`)

- Pre-flight check reads and SHA-256 verifies Chunk `0` before returning HTTP `200`, allowing clean JSON `500 DATA_CORRUPT` (naming `object_id` and `chunk_idx`) or `503 DATA_UNAVAILABLE` if Chunk `0` fails on all replicas.
- Subsequent chunks (`1..N-1`) stream via `StreamingResponse`. `ALIVE` workers are tried before `DEAD` workers; `WorkerRejected` (`404`/`409`) or SHA-256 mismatch atomically marks the replica `CORRUPT`, wakes `trigger_repair_event`, and seamlessly falls back to the surviving replica. Unrecoverable mid-stream corruption critically logs and raises to abort the HTTP transfer rather than delivering truncated bytes.

### Two-Phase Delete (`controller/lifecycle.py`)

`DELETE /api/v1/objects/{id}` rejects `PENDING` objects with `409 OBJECT_BUSY`. For `COMMITTED` objects, a single transaction sets `objects.status = 'DELETING'` and marks all replica rows `PENDING_DELETE` (immediately hiding the object), then issues bounded parallel `DELETE` requests (`WORKER_DELETE_TIMEOUT = 5.0s`, concurrency `16`). Confirmed replica rows are deleted (`finish_delete_if_empty` cascades the object row); unreachable workers retain `PENDING_DELETE` rows for later cleanup on rejoin.

---

## 4. Placement & Replication Strategy

Chunk placement (`controller/placement.py::choose_workers`) queries all `ALIVE` workers alongside their current `OK` replica count and selects the $R = 2$ distinct workers with the fewest chunks, breaking ties randomly (`rng.random()`).

| Scenario | Selection Behavior |
| --- | --- |
| Fresh cluster (`0` chunks) | Uniform random spread across `worker1..worker4`. |
| Skewed cluster load | Prefers least-loaded healthy workers; excludes workers already holding an `OK` or `CORRUPT` copy of the target `chunk_id`. |
| `< 2` healthy eligible workers | Raises `InsufficientWorkersError` (`503 INSUFFICIENT_WORKERS`) on pre-flight or logs deferral during repair. |

---

## 5. Failure Detection & Recovery

### Failure & Rejoin Timeline

```mermaid
sequenceDiagram
    autonumber
    participant W2 as Worker 2
    participant Ctrl as Controller (Liveness & Repair)
    participant DB as SQLite Metadata
    participant W1 as Surviving Worker 1
    participant W3 as Target Worker 3
    participant C as Client

    W2--xCtrl: Worker 2 stops (heartbeats cease)
    Note over Ctrl,DB: Heartbeat age > HEARTBEAT_TIMEOUT (15s)
    Ctrl->>DB: worker2 status: ALIVE -> DEAD (dead_since=t0)
    C->>Ctrl: GET /api/v1/objects/{id}
    Ctrl->>W1: GET /internal/chunks/c0
    W1-->>Ctrl: Verified bytes -> 200 OK
    Note over Ctrl: Past REPAIR_GRACE_PERIOD (now - dead_since >= 30s)
    Ctrl->>W1: GET /internal/chunks/c0 + verify SHA-256 == DB
    Ctrl->>W3: PUT /internal/chunks/c0 (X-Chunk-Checksum)
    W3-->>Ctrl: 201 Created
    Ctrl->>DB: INSERT replica (c0, worker3, OK) -> 2 valid replicas
    W2->>Ctrl: Worker 2 restarts -> POST /internal/workers/register
    Ctrl->>DB: worker2 DEAD -> ALIVE
    Ctrl-->>W2: 200 OK; schedule async reconcile_worker(worker2)
    Ctrl->>W2: GET /internal/workers/worker2/chunks
    Ctrl->>W2: DELETE excess over-replicated chunks -> back to R=2
```

### Replica State Machine

```mermaid
stateDiagram-v2
    [*] --> OK : Upload ACK / Repair Copy / Unrecorded Valid Rejoin Adopted
    OK --> CORRUPT : Read checksum mismatch or 404/409
    OK --> CORRUPT : Scrub bit-rot or DB checksum mismatch
    OK --> CORRUPT : Rejoin inventory checksum mismatch
    OK --> PENDING_DELETE : Object DELETE started (logical delete)
    CORRUPT --> PENDING_DELETE : Replaced while worker was DEAD/unreachable
    CORRUPT --> OK : Worker rejoins with valid disk checksum matching DB
    CORRUPT --> [*] : Deleted on worker disk after repair replacement
    PENDING_DELETE --> [*] : Worker DELETE confirmed (delete, repair, or rejoin)
    OK --> [*] : Deleted on worker / over-replication trimmed / missing on disk
```

- **Startup Grace Window:** On controller restart, `liveness_monitor` suppresses `ALIVE -> DEAD` transitions for `HEARTBEAT_TIMEOUT` (`15s`) so workers can resume heartbeats without false positive failures.
- **Repair Grace Period & Backoff:** Chunks missing solely because a host is `DEAD` wait `REPAIR_GRACE_PERIOD` (`30s`) to absorb brief flaps, whereas `CORRUPT` or missing-row chunks repair immediately via `trigger_repair_event`. Cycles run under a single-flight `asyncio.Lock` capped at `MAX_REPAIRS_PER_CYCLE = 20`, `REPAIR_CONCURRENCY = 4`, and `MAX_CHUNK_REPAIR_ATTEMPTS = 5`.
- **Rejoin Reconciliation:** Returning workers immediately receive `200 OK` on registration while `reconcile_worker` runs asynchronously: clearing `PENDING_DELETE` replicas, adopting unrecorded valid copies if `valid_count < 2`, deleting `DELETING` or expired orphan chunks (`>= ORPHAN_SAFETY_TTL_SECONDS = 600s`), preserving active `PENDING` upload chunks, deleting stale missing-on-disk DB rows, and trimming `valid_count > 2` chunks from highest-loaded workers.

---

## 6. Data Integrity & Scrubbing

- **Layer 1 (Worker Local):** Workers verify `<id>.chunk` against `<id>.sha256` on every `GET` (`409 CHUNK_CORRUPT` on mismatch) and compute a fresh streaming SHA-256 across all stored chunks on `POST /internal/scrub`, yielding `WORKER_SCRUB_YIELD_DELAY = 0.005s` between chunks.
- **Layer 2 (Controller Authoritative):** The controller independently hashes downloaded and repaired chunk payloads against SQLite `chunks.checksum` and cross-checks all `checksums` reported by worker scrubs against DB `chunks.checksum`, detecting simultaneous payload + sidecar tampering.
- **Preserve Unrecoverable Data:** Chunks whose replicas are all `CORRUPT` or `DEAD` are never auto-deleted; they are surfaced explicitly as `unrecoverable` or `unavailable` in `GET /api/v1/admin/replication`.

---

## 7. Deployment, CLI & API Usage

Start the cluster cleanly with `docker compose up -d --build` (or `docker compose down -v && docker compose up -d --build` to reset volumes). All client/admin endpoints are exposed via REST on port `8000` and wrapped by `client/objstore.py` (`upload`, `download`, `info`, `patch`, `list`, `delete`, `workers`, `chunks`, `replication`, `scrub`, with optional `--json` flag). Distributed tracing injects/propagates `X-Request-ID` across controller and worker JSON-lines logs.

---

## 8. Demonstration Results & Measured Timings

Observed on a clean 5-container Docker Compose cluster (`DEMO_NON_INTERACTIVE=1 bash scripts/run_all_demos.sh --non-interactive`):

| Demo Script | Scenario Verified | Measured Result & Timing |
| --- | --- | --- |
| `demo_1_storage.sh` | Text + PNG upload, metadata, download, `cmp` + SHA-256 | PASS (~1s) |
| `demo_2_chunking.sh` | `6,500,000` B -> 6 x `1,048,576` B + `208,544` B tail chunk | PASS (~1s) |
| `demo_3_replication.sh` | 2 distinct `OK` replicas per chunk + disk parity on `worker1..4` | PASS (<1s) |
| `demo_4_metadata.sh` | `PATCH` updates/deletes custom keys with 0 chunk `PUT`s/disk writes | PASS (~2s) |
| `demo_5_failure.sh` | 15 MiB file; `worker2` stopped -> read continuity -> repair after 30s grace -> rejoin trim | PASS (~56s) |
| `demo_6_integrity.sh` | 1-replica bit-rot fallback -> scrub + heal; dual corruption -> `500 DATA_CORRUPT` | PASS (~2s) |
| `check_invariants.py` | Post-demo replication, disk SHA-256 parity, orphan, and TTL check | PASS (exit `0`) |

---

## 9. Testing & Invariant Verification

The test suite is split into `tests/unit/` (isolated unit & hardening tests) and `tests/integration/` (live cluster contract, fault-tolerance, stress, and restart matrix suites):

- **Hardening & Unit Coverage (`tests/unit/`):** Covers DB schema/concurrency (`test_db.py`), placement balance (`test_placement.py`), registry/liveness (`test_api.py`), worker storage crash recovery (`test_storage.py`, `test_worker_http.py`, `test_heartbeat.py`), worker HTTP client timeouts/retries (`test_worker_client.py`, `test_replication.py`), streaming upload/download & lifecycle (`test_upload_download.py`, `test_lifecycle.py`), repair/reconciliation/scrubbing (`test_repair.py`, `test_reconciliation.py`, `test_scrubber.py`), CLI subcommands (`test_cli.py`), and JSON error contracts/input bounds/tracing/SQL parameterization (`test_hardening.py`).
- **Live Integration Coverage (`tests/integration/`):** Verifies all endpoints and boundary sizes (`test_endpoints.py`), worker death/re-replication, fast flaps (< grace period), quorum loss (`503 INSUFFICIENT_WORKERS`), `docker pause` timeout fallback, corruption recovery, outage deletes (`test_fault_tolerance.py`), large 250 MiB streams with bounded controller RSS, `10` concurrent uploads + `10` concurrent downloads, `10` concurrent `PATCH`es, 4-stage cluster restart matrix, and `scripts/check_invariants.py` (`test_stress_and_restarts.py`).

---

## 10. Limitations & Future Work

1. **Controller Single Point of Failure:** The single controller and local SQLite DB limit write scalability and availability; future work could replicate metadata via Raft/rqlite or stateless controllers over PostgreSQL.
2. **Transport Security & Multi-Tenancy:** Internal and public HTTP APIs currently assume a trusted network; adding TLS/mTLS, bearer/HMAC auth, and tenant namespaces would harden production deployment.
3. **Automatic Cluster Rebalancing:** Newly added workers receive new chunks via least-loaded placement, whereas pre-existing chunks are not proactively migrated unless a repair occurs; a low-priority background rebalancer would eliminate long-term skew after node additions.
4. **Garbage Collection of Unrecoverable Objects:** Objects with all replicas permanently `CORRUPT` are preserved indefinitely for operator inspection rather than automatically expired unless manually deleted.

---

## 11. Conclusion

Through streaming 1 MiB chunking, atomic `PENDING -> COMMITTED` transitions, lease-based heartbeats with grace-period re-replication, asynchronous rejoin reconciliation, two-layer SHA-256 verification with automated disk scrubbing, structured `X-Request-ID` observability, and strict invariant verification, the distributed object storage cluster maintains durability and byte-exact integrity across worker crashes, network pauses, silent disk corruption, concurrent load, and full cluster restarts.
