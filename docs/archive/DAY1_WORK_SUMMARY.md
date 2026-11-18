# Day 1 Work Summary: Design and Skeleton

This file records what I did for each Day 1 checklist item. I used `Distributed Object Store Architecture Design.md` as the source. That file is unchanged.

| # | Task | Status | Where |
|---|---|---|---|
| 1 | 1–2 page design doc (components, data model, upload/download/repair flows, failure model) | Done | `docs/design.md` |
| 2 | SQLite schema: `objects`, `chunks`, `replicas(chunk_id, worker_id, state)`, `workers(id, address, last_heartbeat, status)`, `custom_metadata` | Done | `controller/schema.sql` |
| 3 | Repo structure `controller/ worker/ client/ scripts/ tests/ docker-compose.yml` | Done | repo root |
| 4 | Dockerfile per service | Done | `controller/Dockerfile`, `worker/Dockerfile` |
| 5 | Compose: 1 controller, 4 workers (`worker1`–`worker4`), a named volume per worker | Done | `docker-compose.yml` |
| 6 | Health endpoints so all 5 containers start and respond | Written, not run end to end (see below) | `controller/app.py`, `worker/app.py`, `scripts/check_health.sh` |
| 7 | Explain check: draw the upload flow from memory | **Yours to do**, on paper | see the upload flow in `docs/design.md` |

## Details

**Schema.** The architecture doc used different table and column names (`chunk_replicas`, `worker_id`, `host_url`) and kept custom metadata as a JSON column on `objects`. I changed these to match the assignment:
- `replicas` replaces `chunk_replicas` and adds a `state` column (`ACTIVE`, `CORRUPT`, `REPAIRING`).
- `workers` uses `id`, `address`, `last_heartbeat` and `status`.
- `custom_metadata` is now its own table with one row per key.

I also added foreign keys with `ON DELETE CASCADE`, `CHECK` constraints on the status values, and two indexes.

**Naming.** The assignment says `worker1`–`worker4`, so I used those for the compose services, container names, worker IDs and volumes (`worker1-data` and so on). The architecture doc uses `worker-1`. Using `worker1` makes `docker stop worker2` work.

**Services.** I wrote both with the Python standard library only, so the images need no `pip install`. Each service runs only a minimal Day 1 skeleton:
- **Controller:** applies the schema on startup and uses WAL mode. It serves `GET /health`, `POST /internal/workers/register`, `POST /internal/workers/heartbeat` and `GET /internal/workers`.
- **Worker:** serves `GET /health`. A background thread registers with the controller and then sends a heartbeat every 3 s. It retries if the controller isn't up yet. The chunk endpoints are left for Day 2.
- **Client:** `client/client.py` is a stub that only calls the controller's health check.

**Docker and Compose.**
- Each Dockerfile defines a `HEALTHCHECK` that calls `/health`.
- Host ports are `8000` for the controller and `8001`–`8004` for the workers. Every worker listens on 8001 inside its container.
- Compose also creates a `controller-data` volume for the SQLite file.
- `scripts/check_health.sh` curls all 5 ports.

**Tests.** `tests/test_schema.py` has 5 tests: tables exist, schema is idempotent, cascade delete, replicas need an existing worker, and bad status values are rejected.

## Verification
- `python3 -m unittest discover -s tests` passes (5/5).
- `docker compose config -q` passes, so the compose file is valid.
- Both Python services start and print their "listening" line.
- **Not verified:** the actual HTTP health responses and worker registration, and `docker compose up`. My sandbox blocked local network sockets, so I couldn't run them. Please run:
  ```sh
  docker compose up --build -d
  ./scripts/check_health.sh        # expect 5 x OK
  curl localhost:8000/internal/workers   # expect 4 workers
  docker stop worker2              # then re-run the health check; the other 4 still respond
  ```

## Left for you
The explain check (draw the upload flow on paper from memory). Use the upload flow in `docs/design.md` for the steps.

