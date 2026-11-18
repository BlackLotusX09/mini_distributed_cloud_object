# Distributed Object Store

A fault-tolerant, content-verified distributed object storage cluster running 1 FastAPI controller and 4 stateful HTTP chunk workers on Docker Compose with SQLite WAL metadata, 1 MiB chunk striping, 2-way replication across distinct nodes, grace-period re-replication, worker rejoin reconciliation, and two-layer SHA-256 integrity scrubbing.

---

## System Overview & Architecture

```text
  Client / CLI (client/objstore.py) / curl
                    |
                    | REST API (:8000) + X-Request-ID
                    v
    +-------------------------------+
    |       Controller Node         |
    |  FastAPI + uvicorn (:8000)    |
    |  - 1 MiB streaming chunking   |
    |  - Least-loaded R=2 placement |
    |  - Liveness, repair & scrub   |
    +---------------+---------------+
                    |
         SQLite WAL (/data/controller.db)
                    |
    +---------------+---------------+---------------+---------------+
    |               |               |               |               |
    v               v               v               v               |
+--------+     +--------+     +--------+     +--------+             |
|worker1 |     |worker2 |     |worker3 |     |worker4 |  dfs-network|
| :8001  |     | :8002  |     | :8003  |     | :8004  |  (bridge)   |
+---+----+     +---+----+     +---+----+     +---+----+             |
    |              |              |              |                  |
[vol: w1]      [vol: w2]      [vol: w3]      [vol: w4]              |
 /data/chunks   /data/chunks   /data/chunks   /data/chunks          |
```

- **Controller (`controller/`)**: Owns object/chunk/replica metadata in SQLite WAL (`controller-data`), streams uploads/downloads in 1 MiB chunks (`1,048,576` B) without buffering full files in memory, tracks worker liveness, reconciles returning workers, runs bounded background repair, and orchestrates cluster scrubs.
- **Workers (`worker/`)**: Lightweight HTTP chunk stores backed by persistent volumes (`worker1-data`..`worker4-data`). Each chunk `{chunk_id}.chunk` is committed atomically via temp file + `fsync` + `os.replace`, followed by a `.sha256` sidecar file. Workers verify sidecar checksums on every read and during `POST /internal/scrub`.
- **CLI (`client/objstore.py`)**: Dependency-free standard-library CLI providing tabular or `--json` views for all object and admin operations with streaming SHA-256 verification.

---

## Prerequisites & Quick Start

- **Requirements:** Docker Engine & Docker Compose v2, Python 3.10+ (for CLI/tests), `bash`, `curl`.
- **Exposed Ports:** `8000` (controller), `8001` (`worker1`), `8002` (`worker2`), `8003` (`worker3`), `8004` (`worker4`).

```bash
# Build images and start controller + 4 workers in the background
docker compose up --build -d

# Verify controller + all 4 workers report healthy and ALIVE
bash scripts/check_health.sh
python3 client/objstore.py workers
```

### Volume Lifecycle (`down` vs `down -v`)

- `docker compose down`: Stops and removes containers while **preserving named Docker volumes** (`controller-data`, `worker1-data`..`worker4-data`). Restarting with `docker compose up -d` retains all objects, metadata, and chunks.
- `docker compose down -v`: Stops containers **and deletes all named volumes**, wiping the database and all stored chunk files for a clean reset.

---

## Configuration Reference

### Controller Environment Variables (`controller/settings.py`)

| Variable | Default | Operational Effect |
| --- | --- | --- |
| `DB_PATH` | `/data/controller.db` | SQLite database file path (WAL mode + FK enforcement). |
| `HEARTBEAT_TIMEOUT` | `15.0` | Seconds without worker heartbeat before transitioning `ALIVE -> DEAD`. |
| `LIVENESS_CHECK_INTERVAL` | `3.0` | Sweep period (seconds) checking worker heartbeat ages. |
| `REPLICATION_FACTOR` | `2` | Target distinct `ALIVE` replicas per chunk (`R`). |
| `CHUNK_SIZE` | `1048576` | Maximum chunk size in bytes (`1 MiB`). |
| `HEARTBEAT_INTERVAL` | `5` | Heartbeat cadence returned to workers on registration (seconds). |
| `UPLOAD_WORKER_TIMEOUT` | `10.0` | Per-request timeout (s) for controller -> worker `PUT`. |
| `WORKER_READ_TIMEOUT` | `10.0` | Per-request timeout (s) for controller -> worker `GET`. |
| `WORKER_DELETE_TIMEOUT` | `5.0` | Per-request timeout (s) for controller -> worker `DELETE`. |
| `WORKER_SCRUB_TIMEOUT` | `60.0` | Per-request timeout (s) for controller -> worker `POST /internal/scrub`. |
| `MAX_RETRIES_PER_CHUNK` | `3` | Distinct-worker retry rounds during upload replication. |
| `PENDING_OBJECT_TTL_SECONDS` | `600` | Startup janitor age threshold before rolling back stale `PENDING` uploads. |
| `MAX_METADATA_BYTES` | `16384` | Serialized custom metadata JSON size limit per object (`16 KiB`). |
| `REPAIR_GRACE_PERIOD` | `30.0` | Minimum `DEAD` duration (s) before re-replicating chunks from a dead worker. |
| `REPAIR_INTERVAL` | `10.0` | Seconds between scheduled background repair cycles. |
| `MAX_REPAIRS_PER_CYCLE` | `20` | Maximum chunk copies attempted in one repair pass. |
| `REPAIR_CONCURRENCY` | `4` | Concurrent chunk repair copy tasks per cycle (`asyncio.Semaphore`). |
| `MAX_CHUNK_REPAIR_ATTEMPTS` | `5` | Consecutive failed repair attempts before circuit-breaker backoff skips a chunk. |
| `ORPHAN_SAFETY_TTL_SECONDS` | `600` | Age threshold before deleting unknown on-disk orphan chunks at worker rejoin. |
| `SCRUB_INTERVAL` | `300.0` | Seconds between full-cluster automated scrub passes. |
| `WORKER_SCRUB_YIELD_DELAY` | `0.005` | Delay between chunks during worker disk scrubbing. |
| `LOG_LEVEL` | `INFO` | Controller JSON-lines log threshold (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

### Worker Environment Variables (`worker/config.py`)

| Variable | Default | Operational Effect |
| --- | --- | --- |
| `WORKER_ID` | *(required)* | Unique node identifier (e.g., `worker1`), used in internal advertised URL. |
| `PORT` | `8000` | Internal HTTP server port inside container. |
| `DATA_DIR` | `/data` | Persistent storage root (`$DATA_DIR/chunks/<prefix>/<chunk_id>.chunk`). |
| `CONTROLLER_URL` | `http://controller:8000` | Controller base URI for registration and heartbeats. |
| `HEARTBEAT_INTERVAL` | `5s` | Interval duration (`500ms`, `5s`, `1m`) between worker heartbeats. |
| `WORKER_SCRUB_YIELD_DELAY` | `0.005` | Seconds slept between chunks during `POST /internal/scrub`. |
| `LOG_LEVEL` | `INFO` | Worker JSON-lines log threshold. |

---

## REST API & CLI Reference

Every non-2xx response across controller and worker returns JSON: `{"error": "<ERROR_CODE>", "message": "<detail>"}` and echoes `X-Request-ID`.

### Public Object & Admin Endpoints

| Endpoint | Method | CLI Equivalent (`client/objstore.py`) | Success Status |
| --- | --- | --- | --- |
| `/api/v1/objects` | `POST` | `upload <FILE> [--meta k=v ...]` | `201 Created` |
| `/api/v1/objects/{object_id}` | `GET` | `download <OBJECT_ID> [-o PATH]` | `200 OK` |
| `/api/v1/objects/{object_id}/metadata` | `GET` | `info <OBJECT_ID>` | `200 OK` |
| `/api/v1/objects/{object_id}/metadata` | `PATCH` | `patch <OBJECT_ID> <k=v ...>` | `200 OK` |
| `/api/v1/objects` | `GET` | `list [--limit N] [--offset M]` | `200 OK` |
| `/api/v1/objects/{object_id}` | `DELETE` | `delete <OBJECT_ID>` | `204 No Content` |
| `/api/v1/admin/workers` | `GET` | `workers` | `200 OK` |
| `/api/v1/admin/objects/{object_id}/chunks` | `GET` | `chunks <OBJECT_ID>` | `200 OK` |
| `/api/v1/admin/replication` | `GET` | `replication` | `200 OK` |
| `/api/v1/admin/scrub` | `POST` | `scrub` | `200 OK` |
| `/health` | `GET` | — | `200 OK` |

### Internal Worker / Controller Endpoints

- **Controller internal:** `POST /internal/workers/register`, `POST /internal/workers/heartbeat`
- **Worker internal:** `PUT /internal/chunks/{chunk_id}`, `GET /internal/chunks/{chunk_id}`, `DELETE /internal/chunks/{chunk_id}`, `GET /internal/workers/{worker_id}/chunks`, `POST /internal/scrub`, `GET /health`

### Examples (`curl` & CLI)

```bash
# Upload with custom metadata
curl -sf -X POST http://localhost:8000/api/v1/objects \
  -F "file=@demo/files/sample.txt" \
  -F 'metadata={"env":"dev","owner":"alice"}'
# CLI equivalent (--json optional on any subcommand)
python3 client/objstore.py upload demo/files/sample.txt --meta env=dev --meta owner=alice

# Merge-patch metadata (delete key with null / empty value)
curl -sf -X PATCH http://localhost:8000/api/v1/objects/<OBJECT_ID>/metadata \
  -H 'Content-Type: application/json' -d '{"env":"prod","owner":null}'
python3 client/objstore.py patch <OBJECT_ID> env=prod owner=null

# Download and verify SHA-256
curl -sf http://localhost:8000/api/v1/objects/<OBJECT_ID> -o out.txt
python3 client/objstore.py download <OBJECT_ID> -o out.txt

# Admin health, chunk placement & manual scrub
python3 client/objstore.py workers
python3 client/objstore.py chunks <OBJECT_ID>
python3 client/objstore.py replication
python3 client/objstore.py scrub
```

Sample upload response (`201 Created`):

```json
{
  "object_id": "fa2f7fee-8ceb-47c9-9599-113e5d77a373",
  "name": "sample.txt",
  "size": 1976,
  "content_type": "text/plain",
  "checksum": "ff14ab46194e5323fe27731d16a6cb96441dfdb8e9deb4613e85e5facc8428f4",
  "chunk_count": 1,
  "created_at": 1795020417.12,
  "modified_at": 1795020417.12,
  "metadata": {"env": "dev", "owner": "alice"}
}
```

### HTTP Error Status Codes

| Status | Error Code(s) | Trigger |
| --- | --- | --- |
| `400` | `MISSING_FILE`, `INVALID_METADATA`, `INVALID_PAGINATION`, `INVALID_INPUT`, `INVALID_REQUEST` | Missing upload file, bad metadata JSON/types/>64 keys/>16 KiB/reserved key, bad `limit`/`offset`, object name > 255 chars, malformed JSON/headers. |
| `404` | `OBJECT_NOT_FOUND`, `NOT_FOUND`, `UNKNOWN_WORKER` | Unknown or malformed (`!UUID4`) `object_id`, `PENDING`/`DELETING` object access, unknown URL path, heartbeat from unregistered worker. |
| `405` | `METHOD_NOT_ALLOWED` | Unsupported HTTP method on a valid route (e.g., `PUT /api/v1/objects`). |
| `409` | `OBJECT_BUSY` | `DELETE /api/v1/objects/{id}` attempted while upload is still `PENDING`. |
| `500` | `DATA_CORRUPT`, `INTERNAL_ERROR` | All available replicas of Chunk 0 are corrupt; unhandled server exception (stack trace sanitized). |
| `503` | `INSUFFICIENT_WORKERS`, `STORAGE_UNAVAILABLE`, `DATA_UNAVAILABLE` | Fewer than 2 `ALIVE` workers before upload, upload replication failure after retries, all replicas unreachable on Chunk 0 read. |

---

## Automated Demo Suite

Generate fixtures and run all 6 end-to-end demonstrations plus invariant checks:

```bash
# Run all 6 demos non-interactively (~60-70s total, including worker death & grace period)
DEMO_NON_INTERACTIVE=1 bash scripts/run_all_demos.sh --non-interactive

# Run individual demos interactively
bash scripts/demo_1_storage.sh       # (~1s) Upload/download text & PNG, cmp + SHA-256 check
bash scripts/demo_2_chunking.sh      # (~1s) 6,500,000 B -> 6x 1 MiB + 208,544 B tail chunk
bash scripts/demo_3_replication.sh   # (~1s) R=2 distinct workers + on-disk container file parity
bash scripts/demo_4_metadata.sh      # (~2s) GET/PATCH metadata without worker PUT traffic
bash scripts/demo_5_failure.sh       # (~56s) Stop worker2, grace-period repair to R=2, rejoin reconcile
bash scripts/demo_6_integrity.sh     # (~2s) Inject bit-rot via corrupt_chunk.sh, read fallback, scrub & heal
```

---

## Architecture & Design Trade-offs

1. **Streaming 1 MiB Chunking:** Controller reads and uploads at most `1,048,576` bytes per chunk in flight (and downloads one chunk per iteration via `StreamingResponse`), bounding memory even on 250+ MiB streams.
2. **Least-Loaded Distinct Placement (`controller/placement.py`):** Ranks `ALIVE` workers by lowest active `OK` replica count with random tie-breaking while excluding existing holders of a chunk, ensuring distinctness and even spread without a consistent hashing ring.
3. **Atomic `PENDING -> COMMITTED` Visibility:** Uploads create a `PENDING` object row first and become visible to public reads, listings, and `PATCH` only once every chunk has 2 acknowledged replicas; mid-upload failures trigger immediate best-effort rollback plus a startup janitor after `PENDING_OBJECT_TTL_SECONDS`.
4. **Heartbeat + Repair Grace Period:** Workers are marked `DEAD` after `15s` without heartbeats, while re-replication waits `REPAIR_GRACE_PERIOD = 30s` so brief restarts/network blips do not trigger unnecessary 1 MiB chunk copies.
5. **Idempotent Bounded Repair & Rejoin Reconciliation:** A single-flight `asyncio.Lock`, `MAX_REPAIRS_PER_CYCLE = 20`, and `REPAIR_CONCURRENCY = 4` cap background repair bandwidth. Rejoining workers reconcile disk inventory asynchronously, clearing `PENDING_DELETE` rows, adopting valid unrecorded under-replicated chunks, keeping active `PENDING`/TTL-protected uploads, and trimming over-replicated chunks back to `R = 2`.
6. **Two-Layer Integrity & Rate-Limited Scrubbing:** Every worker read verifies the `.chunk` payload against its local `.sha256` sidecar (`409 CHUNK_CORRUPT` on mismatch), and the controller independently re-hashes bytes against DB `chunks.checksum` before streaming. Periodic `POST /internal/scrub` sleeps `0.005s` between chunks and cross-checks sidecar hashes against DB checksums to catch simultaneous payload + sidecar tampering.

---

## Testing & Invariant Verification

```bash
# 1. Fast unit tests (no Docker required)
.venv/bin/python -m pytest tests/unit -v

# 2. Fast integration tests against live Docker Compose stack (or local processes)
.venv/bin/python -m pytest tests/integration -m "not slow" -v

# 3. Full integration suite (includes slow failure recovery, 250 MiB stream, 10x10 concurrency, restarts)
.venv/bin/python -m pytest tests/integration -v

# 4. Standalone cluster invariant checker (checks R=2 distinctness, disk SHA-256 parity, orphans, TTL)
python3 scripts/check_invariants.py
```

---

## Known Limitations & Troubleshooting

- **Single Controller SPOF & SQLite Serialization:** Metadata lives in a single SQLite WAL database on the `controller` node. While `BEGIN IMMEDIATE` + `busy_timeout = 5000` prevents lost updates under concurrency, high-write throughput is bounded by single-writer SQLite transactions.
- **No Authentication or TLS:** Designed for an isolated bridge network (`dfs-network`) inside Docker without client auth or mTLS.
- **No Automatic Rebalance on Scale-Out:** Adding a 5th worker directs new placements and repairs to it via least-loaded selection, but existing healthy chunks remain on their original nodes.
- **Troubleshooting:**
  - *Workers stuck in registration/not ALIVE:* Inspect `docker compose logs controller worker1` and verify ports `8000–8004` are free on localhost.
  - *Cleanup after aborted tests:* Run `docker compose start worker1 worker2 worker3 worker4` or `docker compose down -v && docker compose up -d --build` for a fresh slate.
