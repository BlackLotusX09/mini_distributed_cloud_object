# Day 3: Controller Core Summary

Built the controller's metadata layer, worker registry, liveness monitor and placement engine. The upload/download data paths are not built yet.

## Files
| File | Purpose |
|---|---|
| `controller/db.py` | Pure sqlite3 layer, no HTTP: fresh connection per call, pragmas, `transaction()`, schema, worker/object/chunk/replica operations |
| `controller/placement.py` | `choose_workers()`, a pure function with no DB imports, and `InsufficientWorkersError` |
| `controller/settings.py` | Env config: `DB_PATH`, `HEARTBEAT_TIMEOUT`, `LIVENESS_CHECK_INTERVAL`, `REPLICATION_FACTOR`, `CHUNK_SIZE` |
| `controller/main.py` | FastAPI app: lifespan, error contract, worker endpoints, liveness monitor, admin endpoints |
| `controller/Dockerfile`, `requirements.txt` | uvicorn image (installs fastapi + uvicorn) |
| `docker-compose.yml` | The `controller-data` named volume is now mounted at `/data`; new env vars are set |
| `tests/test_db.py`, `test_placement.py`, `test_api.py` | 76 new tests |
| `scripts/local_smoke.sh` | 20 integration checks without Docker |

Removed (superseded): `controller/app.py`, `controller/schema.sql`, `tests/test_schema.py`.

## Database (`db.py`)
- Every call opens a new connection with `foreign_keys=ON`, `journal_mode=WAL` and `busy_timeout=5000`. Connections are never shared across threads.
- `transaction(conn)` runs `BEGIN IMMEDIATE`, commits on a clean exit and rolls back on any exception. Every write is routed through it. A caller can pass its own connection to group steps.
- Schema is exactly the one you confirmed. It has `CHECK` constraints on the status columns and a `UNIQUE(object_id, idx)` index, plus the indexes on `replicas(state)`, `replicas(worker_id)`, `(state, worker_id)`, `workers(status)` and `objects(status)`.
- Operations:
  - Workers: `upsert_worker` (returns `new` or `returning`), `record_heartbeat` (returns the previous status, or `None` if the worker is unknown), `get_worker`, `list_workers`, `mark_dead`, `mark_alive`.
  - Objects and replicas: `create_object_pending`, `add_chunk`, `add_replica`, `commit_object`, `get_object` (with ordered chunks and their replicas), `delete_object` (cascades), `count_chunks_per_worker` (counts `OK` replicas only).
- `mark_dead(..., stale_before=)` only marks a worker dead if its last heartbeat is older than the cutoff. This avoids a false DEAD when a heartbeat arrives mid-sweep.

## Controller API (`main.py`)
- All errors return `{"error", "message"}`: 400 `INVALID_REQUEST`, 404 `UNKNOWN_WORKER`, 503 `INSUFFICIENT_WORKERS`. I also mapped framework 404/405 and unexpected 500 errors into the same shape.
- All timestamps come from the controller's own `time.time()`. A `timestamp` sent by a worker is ignored (tested).
- `POST /internal/workers/register` validates the body, upserts, and logs `new` or `returning (was DEAD)`. It returns `{"status":"registered","heartbeat_interval":5}`.
- `POST /internal/workers/heartbeat` returns 404 for an unknown worker. It logs `DEAD -> ALIVE` only on a transition and does not log routine heartbeats.
- `GET /api/v1/admin/workers` returns `worker_id, address, status, last_heartbeat_age, dead_since, chunk_count`.
- `GET /api/v1/admin/placement-test` is the temporary endpoint. It returns `{"replication_factor", "workers": [...]}` or 503.
- `GET /health` is kept for Docker and the health scripts.

## Liveness monitor
- It runs as an asyncio task started and cancelled in `lifespan`. Each sweep runs in `asyncio.to_thread`, so the event loop is never blocked. Each iteration is wrapped in `try/except`, so a transient DB error is logged and the loop continues.
- **Grace period:** no worker is marked DEAD during the first `HEARTBEAT_TIMEOUT` seconds after startup. It uses a monotonic clock.
- After the grace period, every `LIVENESS_CHECK_INTERVAL` it marks ALIVE workers with `now - last_heartbeat > HEARTBEAT_TIMEOUT` as DEAD. It logs only the transition, e.g. `worker2 ALIVE -> DEAD (no heartbeat for 6s)`.
- Replicas are never changed when a worker goes DEAD (tested).

## Placement
`choose_workers(alive_workers, n, exclude=())` accepts `{id: count}` or a list of dicts with `worker_id`, `chunk_count` and `status`. It drops non-ALIVE and excluded workers and sorts by fewest chunks, with a random tie-break among equal counts. It raises `InsufficientWorkersError(needed, available)` when there aren't enough eligible workers. A global handler turns that into 503 `need N healthy workers, have M`.

## Verification
| Check | Result |
|---|---|
| `.venv/bin/python -m pytest tests -q` (all tests, including Day 2 worker tests) | 120 passed |
| `bash scripts/local_smoke.sh` (real uvicorn controller + 4 worker processes) | 20 of 20 passed |
| `docker compose up --build` with the new controller image | **Not run:** the Docker daemon was not running |

Notable tests:
- Upserting a worker twice keeps one row.
- Foreign keys are enforced.
- The `replicas` composite primary key is enforced.
- A rollback leaves no partial rows.
- Delete cascades.
- Data survives close and reopen.
- Placement is balanced.
- The grace period and DEAD/ALIVE transitions work.
- The monitor survives errors.
- Controller state survives a restart.

## Things to know
- **Balance test interpretation:** the "no worker gets more than ~35%" check counts each worker's share of replica assignments. With 1,000 placements of 2 replicas that is 2,000 slots. Counting "placements that include a worker" would give about 50% by definition. With least-chunks-first the shares come out within 1 chunk of each other.
- **Heartbeat timing:** workers beat every 5 s and the timeout is 15 s, so a worker survives two missed heartbeats. The compose file passes `HEARTBEAT_TIMEOUT=15`.
- **Orphaned old DB:** the old `/data/metadata/metadata.db` on the existing `controller-data` volume is no longer used. The new DB is `/data/controller.db` on the same volume. Workers re-register on their own.
- **Not stored yet:** `free_bytes` and `chunk_count` from workers are validated but not saved. The spec's `workers` table has no columns for them.
- **Docker verify script:** `scripts/docker_verify.sh` is updated for the new JSON format and admin endpoint, but not re-run since the controller change.
- **`.venv/`:** I created it in the project for local testing and installed the dependencies with your approval. It's safe to delete or git-ignore.

To check the Docker stack, start Docker Desktop, then:
```sh
docker compose up --build -d && bash scripts/docker_verify.sh
```

