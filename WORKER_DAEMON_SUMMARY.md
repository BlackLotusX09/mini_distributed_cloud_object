# Worker Daemon: Work Summary

Implements the worker storage node spec: a dumb, crash-resilient chunk store with no knowledge of objects or replication.

## Layout
```
worker/
  config.py            env-var config (WORKER_ID, DATA_DIR, CONTROLLER_URL, PORT, HEARTBEAT_INTERVAL)
  storage/engine.py    transport-free storage engine (no HTTP)
  storage/__init__.py  public API
  heartbeat.py         register (backoff) + heartbeat daemon
  app.py               HTTP daemon wrapping the storage engine
  Dockerfile
tests/
  test_storage.py      23 unit tests incl. real SIGKILL mid-write
  test_worker_http.py  12 HTTP API tests
  test_heartbeat.py    9 config + heartbeat/registration tests
  test_schema.py       5 schema tests (Day 1)
scripts/docker_verify.sh   40 end-to-end checks against the compose stack
```

## Storage engine
- **IDs:** `fullmatch` on `[A-Za-z0-9_-]{1,128}`. Anything else raises `InvalidChunkIDException` before any path is built.
- **Layout:** `DATA_DIR/chunks/<first-2-chars>/<id>.chunk` plus a `<id>.sha256` sidecar.
- **Atomic write:**
  1. Write `.<id>.chunk.tmp` in the same directory, so it is on the same filesystem.
  2. `flush()`, then `fsync()`.
  3. `os.replace()` it to `<id>.chunk`.
  4. Do the same for the checksum sidecar. It is written last, so it is the commit marker.
  5. `fsync` the directory.
- **Crash recovery:** on startup all `*.tmp` files are unlinked. A `.chunk` file without a valid sidecar counts as an aborted write. It reads as `NotFound` and is excluded from listings. A retry overwrites it.
- **`write_chunk`:**
  - An `expected_checksum` mismatch aborts before any disk access.
  - Identical content returns `ALREADY_EXISTS`, but only after the stored bytes are re-verified. If the stored copy is damaged, the write rewrites it.
  - Different content under the same ID raises `ChunkConflictError`.
- **`read_chunk`:** requires both files. It recomputes the SHA-256 and compares it with `hmac.compare_digest`. A mismatch or a malformed sidecar raises `ChunkCorruptionError`, and no bytes are returned.
- **`delete_chunk`:** unlinks the sidecar first, then the payload. It is idempotent.
- **`list_chunks`:** returns `{chunk_id, size, checksum}` and skips tmp files and aborted writes.
- **Concurrency:** striped per-chunk locks. A test with 16 concurrent identical PUTs gets exactly one `CREATED`.

## HTTP API
| Condition | Status | `error` |
|---|---|---|
| Bad id | 400 | `INVALID_CHUNK_ID` |
| Checksum mismatch | 400 | `CHECKSUM_MISMATCH` |
| Over 1 MiB | 400 | `CHUNK_TOO_LARGE` |
| Missing | 404 | `CHUNK_NOT_FOUND` |
| On-disk corruption | 409 | `CHUNK_CORRUPT` |
| Foreign worker list | 404 | `WORKER_NOT_FOUND` |

These are extras beyond the spec's matrix:
- 409 `CHUNK_CONFLICT` when an ID already exists with different content.
- 411 `LENGTH_REQUIRED` when `Content-Length` is missing.
- 405 for a wrong method.
- 500 `STORAGE_ERROR` / `INTERNAL_ERROR` for I/O or unexpected errors.

All error bodies are `{"error", "message"}` JSON.

Endpoints: `PUT/GET/DELETE /internal/chunks/{id}`, `GET /internal/workers/{worker_id}/chunks`, `GET /health`.

- **PUT:** returns 201 for a new chunk and 200 for an idempotent repeat. An oversized body is rejected from its `Content-Length` before it is stored.
- **GET:** sends `X-Chunk-Checksum`.
- **DELETE:** always returns 204.
- **Health:** `{worker_id, chunk_count, free_bytes, status: "healthy"}`.

## Heartbeat daemon
- Registers with `{worker_id, address, free_bytes, chunk_count}`.
- Retries with exponential backoff (1 s, 2 s, 4 s, capped at 10 s). It never crashes while the controller is down.
- Sends `{worker_id, timestamp, free_bytes, chunk_count}` every `HEARTBEAT_INTERVAL`, default 5 s.
- Heartbeat errors keep the same cadence. A 404 from the controller triggers re-registration.
- The advertised address is always `http://<WORKER_ID>:<PORT>`, never localhost.
- Handles `SIGTERM`, so `docker stop` is fast.

## Changes outside `worker/`
- **Controller:**
  - Register and heartbeat accept `worker_id`, with the old `id` still accepted.
  - A heartbeat from an unknown worker returns 404 `WORKER_UNKNOWN`, so the re-register path actually works.
  - Free-space and chunk-count values are accepted but not stored yet, because the Day 1 `workers` schema has no columns for them.
- **Compose:** workers now listen on container port **8000** and mount each volume at **/data**, per the spec. Host ports are still 8001–8004. Each worker also gets `DATA_DIR` and `HEARTBEAT_INTERVAL` env vars.
- **Day 1 doc:** `DAY1_WORK_SUMMARY.md` still mentions the old worker port 8001 and the `/data/chunks` mount. Both are now outdated.

## Verification results
| Check | Result |
|---|---|
| Unit and HTTP tests (`python3 -m unittest discover -s tests`) | 49 of 49 passed |
| `scripts/docker_verify.sh` against the real compose stack | 40 of 40 passed |
| `docker compose ps` | all 5 containers `healthy` |

Checklist coverage:
- **Atomicity:** a real `SIGKILL` of a child process mid-write. A kill before the payload rename leaves only a tmp file. A kill between the payload and checksum renames leaves a payload with no checksum. In both cases the chunk reads as missing, and the tmp files are cleaned on restart. A retry then succeeds.
- **Checksum enforcement:** a single bit flip on disk gives 409 `CHUNK_CORRUPT` with no chunk bytes (unit tests and in Docker).
- **Size boundary:** 1,048,576 bytes succeeds. 1,048,577 bytes gets 400 `CHUNK_TOO_LARGE` and is not stored.
- **Path traversal:** `../`, `%2e%2e%2f`, `a/b`, `a.b`, spaces, null bytes, an empty ID and a 129-character ID all get 400 `INVALID_CHUNK_ID`.
- **Restart and mount resilience:** data and catalog survive `docker restart`, `docker kill` (SIGKILL) and `docker compose down` followed by `up`, without `-v`. A corrupt chunk is still refused after a restart.
- **Volume isolation:** a chunk written to worker1 is not visible on worker2, from the API or the filesystem.

## Notes
- The checks for the 5 s heartbeat against the controller's 10 s timeout rely on at most one missed beat before a worker is marked dead.
- On a case-insensitive filesystem such as default macOS APFS, the IDs `Abc` and `abc` would collide when running outside Docker. Inside the Linux container they don't.
