# Testing Guide

Run everything from the project root. Docker Desktop must be running for sections 2–8.

## 1. Automated tests (no Docker needed)
One-time setup (a virtualenv with the controller and test dependencies):
```sh
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```
```sh
.venv/bin/python -m pytest tests -q          # expect: 120 passed
```
To run only some of them:
```sh
.venv/bin/python -m pytest tests/test_db.py -v          # SQLite layer (FKs, cascade, rollback, persistence)
.venv/bin/python -m pytest tests/test_placement.py -v   # placement engine (pure function)
.venv/bin/python -m pytest tests/test_api.py -v         # controller API, liveness monitor, grace period
.venv/bin/python -m pytest tests/test_storage.py tests/test_worker_http.py tests/test_heartbeat.py -v   # worker
```

## 1b. Integration check without Docker (real controller + 4 workers as processes)
```sh
bash scripts/local_smoke.sh      # expect: passed=20 failed=0 (~35 s; uses ports 8000-8004)
```
It covers registration, the error contract, placement, killing a worker (DEAD), restarting it (ALIVE),
and a controller restart that keeps state with no false DEAD workers.

## 1c. Controller admin endpoints (stack running)
```sh
curl -s localhost:8000/api/v1/admin/workers | python3 -m json.tool   # worker_id, address, status, last_heartbeat_age, dead_since, chunk_count
curl -s localhost:8000/api/v1/admin/placement-test                   # {"replication_factor":2,"workers":[two distinct ALIVE ids]}
docker stop worker2; sleep 20                                        # default timeout is 15 s
curl -s localhost:8000/api/v1/admin/workers | python3 -m json.tool   # worker2 DEAD with dead_since set
docker compose logs controller | grep -E 'ALIVE -> DEAD|DEAD -> ALIVE|registered'
docker start worker2; sleep 8                                        # worker2 back to ALIVE
docker compose stop worker1 worker2 worker3; sleep 20
curl -i localhost:8000/api/v1/admin/placement-test                   # 503 INSUFFICIENT_WORKERS "need 2 healthy workers, have 1"
docker compose start worker1 worker2 worker3
curl -i -X POST localhost:8000/internal/workers/heartbeat -H 'Content-Type: application/json' -d '{"worker_id":"ghost"}'   # 404 UNKNOWN_WORKER
curl -i -X POST localhost:8000/internal/workers/register  -H 'Content-Type: application/json' -d '{"worker_id":"x"}'       # 400 INVALID_REQUEST
```

## 2. Start the stack
```sh
docker compose up --build -d
docker compose ps                  # expect 5 containers "healthy" after ~10-15 s
bash scripts/check_health.sh       # expect 5 x OK
curl -s localhost:8000/api/v1/admin/workers   # expect 4 workers, address http://workerN:8000
```
Ports: controller `8000`, worker1–4 `8001`–`8004`.

## 3. One-shot end-to-end check (all of the below, automated)
```sh
bash scripts/docker_verify.sh      # expect: passed=40 failed=0 (takes ~1 min; restarts containers)
```

## 4. Manual API tests (use worker1 = port 8001)
Prepare test data:
```sh
head -c 4096 /dev/urandom > /tmp/small.bin
SUM=$(shasum -a 256 /tmp/small.bin | cut -d' ' -f1)
```

| # | Test | Command | Expected |
|---|---|---|---|
| 1 | Upload new chunk | `curl -i -X PUT -H "X-Chunk-Checksum: $SUM" --data-binary @/tmp/small.bin localhost:8001/internal/chunks/t1` | `201`, JSON `{chunk_id,size,checksum}` |
| 2 | Idempotent upload | same command again | `200` |
| 3 | Download + verify | `curl -s localhost:8001/internal/chunks/t1 \| cmp - /tmp/small.bin && echo SAME` | `SAME` |
| 4 | Checksum header | `curl -si localhost:8001/internal/chunks/t1 \| grep -i x-chunk-checksum` | equals `$SUM` |
| 5 | Wrong checksum | `curl -i -X PUT -H "X-Chunk-Checksum: deadbeef" --data-binary @/tmp/small.bin localhost:8001/internal/chunks/t2` | `400` `CHECKSUM_MISMATCH`; `t2` not stored |
| 6 | Same ID, different data | `curl -i -X PUT --data-binary other localhost:8001/internal/chunks/t1` | `409` `CHUNK_CONFLICT` |
| 7 | Missing chunk | `curl -i localhost:8001/internal/chunks/nope` | `404` `CHUNK_NOT_FOUND` |
| 8 | Delete | `curl -i -X DELETE localhost:8001/internal/chunks/t1` | `204` |
| 9 | Delete again | same command again | `204` (idempotent) |
| 10 | List chunks | `curl -s localhost:8001/internal/workers/worker1/chunks` | `{"worker_id":"worker1","chunks":[...]}` |
| 11 | List for wrong worker | `curl -i localhost:8001/internal/workers/worker2/chunks` | `404` `WORKER_NOT_FOUND` |
| 12 | Health | `curl -s localhost:8001/health` | `{"worker_id","chunk_count","free_bytes","status":"healthy"}` |

## 5. Size boundary
```sh
head -c 1048576 /dev/urandom > /tmp/max.bin
head -c 1048577 /dev/urandom > /tmp/over.bin
curl -i -X PUT --data-binary @/tmp/max.bin  localhost:8001/internal/chunks/maxok   # 201
curl -i -X PUT --data-binary @/tmp/over.bin localhost:8001/internal/chunks/overbad # 400 CHUNK_TOO_LARGE
curl -i localhost:8001/internal/chunks/overbad                                     # 404 (not stored)
```

## 6. Path traversal / invalid IDs (all must be `400 INVALID_CHUNK_ID`)
```sh
curl -i --path-as-is "localhost:8001/internal/chunks/../etc/passwd"
curl -i --path-as-is "localhost:8001/internal/chunks/%2e%2e%2fsecret"
curl -i "localhost:8001/internal/chunks/a.b"
curl -i "localhost:8001/internal/chunks/a%20b"
curl -i -X PUT --data-binary hi "localhost:8001/internal/chunks/$(printf 'x%.0s' $(seq 1 129))"
```

## 7. Corruption detection (bit flip)
```sh
curl -s -o /dev/null -X PUT --data-binary @/tmp/small.bin localhost:8001/internal/chunks/flipme
docker exec worker1 python -c "
p='/data/chunks/fl/flipme.chunk'
b=bytearray(open(p,'rb').read()); b[100]^=1; open(p,'wb').write(bytes(b))"
curl -i localhost:8001/internal/chunks/flipme     # expect 409 CHUNK_CORRUPT, JSON body, no chunk bytes
# repair: delete then re-upload
curl -s -X DELETE localhost:8001/internal/chunks/flipme
curl -s -o /dev/null -X PUT --data-binary @/tmp/small.bin localhost:8001/internal/chunks/flipme
curl -s localhost:8001/internal/chunks/flipme | cmp - /tmp/small.bin && echo REPAIRED
```

## 8. Resilience and isolation
```sh
# Volume isolation: a chunk on worker1 must not exist on worker2
curl -s -o /dev/null -X PUT --data-binary @/tmp/small.bin localhost:8001/internal/chunks/iso1
curl -i localhost:8002/internal/chunks/iso1       # expect 404

# Restart, hard kill and compose down (without -v) must keep data
curl -s -o /dev/null -X PUT --data-binary @/tmp/small.bin localhost:8003/internal/chunks/keep1
docker restart worker3
docker kill worker3 && docker start worker3       # SIGKILL
docker compose down && docker volume ls | grep worker3-data   # volume still there
docker compose up -d
sleep 15
curl -s localhost:8003/internal/chunks/keep1 | cmp - /tmp/small.bin && echo SURVIVED

# Stop one worker; the others stay up and it rejoins
docker stop worker2
bash scripts/check_health.sh                      # :8002 FAIL, others OK
docker start worker2; sleep 8
bash scripts/check_health.sh                      # 5 x OK
curl -s localhost:8000/api/v1/admin/workers   # worker2 ALIVE again
```
Registration check (no localhost advertised): `curl -s localhost:8000/api/v1/admin/workers | grep -c localhost` → `0`.

## 9. Inspecting logs and disk
```sh
docker compose logs worker1 --tail 20             # requests, "registered as http://worker1:8000"
docker exec worker1 find /data -type f            # see chunk/.sha256 pairs, sharded by 2 chars
```
After a crash you may see `.<id>.chunk.tmp`; they are removed on the next start (log line `recovered_tmp=N`).

## 10. Cleanup
```sh
docker compose down          # keeps data volumes
docker compose down -v       # also deletes all volumes (data loss, wipes all chunks)
```

