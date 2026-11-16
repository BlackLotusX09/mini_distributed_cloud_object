#!/usr/bin/env bash
# Integration check WITHOUT Docker: real uvicorn controller + 4 real workers as local processes.
# Needs: .venv with controller/requirements.txt installed. Uses ports 8000-8004.
cd "$(dirname "$0")/.." || exit 1
PY="$PWD/.venv/bin/python"
T=/tmp/ds_t; rm -rf $T; mkdir -p $T
pass=0; fail=0
ok()  { echo "PASS  $1"; pass=$((pass+1)); }
bad() { echo "FAIL  $1  ($2)"; fail=$((fail+1)); }
expect() { [ "$2" = "$3" ] && ok "$1" || bad "$1" "expected $2, got $3"; }
status_of() { curl -s localhost:8000/api/v1/admin/workers | $PY -c "import sys,json; d={w['worker_id']:w for w in json.load(sys.stdin)}; print(d.get('$1',{}).get('$2'))"; }
wait_status() { for _ in $(seq 1 "${4:-30}"); do [ "$(status_of "$1" status)" = "$2" ] && return 0; sleep 1; done; return 1; }

start_controller() {
  DB_PATH=$T/controller.db HEARTBEAT_TIMEOUT=6 LIVENESS_CHECK_INTERVAL=1 \
    $PY -m uvicorn --app-dir controller main:app --port 8000 --log-level warning >>$T/controller.log 2>&1 &
  CPID=$!
}
start_worker() {
  (cd worker && WORKER_ID=worker$1 PORT=800$1 DATA_DIR=$T/w$1 CONTROLLER_URL=http://localhost:8000 \
    HEARTBEAT_INTERVAL=1s exec $PY app.py >>$T/w$1.log 2>&1) &
  WPID[$1]=$!
}
declare -a WPID
cleanup() { kill $CPID ${WPID[@]} 2>/dev/null; }
trap cleanup EXIT

start_controller
for i in 1 2 3 4; do start_worker $i; done
for _ in $(seq 1 20); do curl -sf localhost:8000/health >/dev/null && break; sleep 0.5; done
sleep 3

echo "--- registration"
expect "controller /health" ok "$(curl -s localhost:8000/health | $PY -c 'import sys,json; print(json.load(sys.stdin)["status"])')"
for i in 1 2 3 4; do expect "worker$i ALIVE" ALIVE "$(status_of worker$i status)"; done
expect "worker1 advertises bridge name" "http://worker1:8001" "$(status_of worker1 address)"

echo "--- error contract"
r=$(curl -s -w ' %{http_code}' -X POST localhost:8000/internal/workers/heartbeat -H 'Content-Type: application/json' -d '{"worker_id":"ghost"}')
echo "$r" | grep -q 'UNKNOWN_WORKER.* 404$' && ok "heartbeat for unknown worker -> 404 UNKNOWN_WORKER" || bad "unknown worker" "$r"
r=$(curl -s -w ' %{http_code}' -X POST localhost:8000/internal/workers/register -H 'Content-Type: application/json' -d '{"worker_id":"x"}')
echo "$r" | grep -q 'INVALID_REQUEST.* 400$' && ok "register without address -> 400 INVALID_REQUEST" || bad "invalid request" "$r"

echo "--- placement"
p=$(curl -s localhost:8000/api/v1/admin/placement-test)
n=$(echo "$p" | $PY -c 'import sys,json; print(len(set(json.load(sys.stdin)["workers"])))')
expect "placement-test returns 2 distinct workers" 2 "$n"

echo "--- liveness: kill worker2"
kill ${WPID[2]}; wait ${WPID[2]} 2>/dev/null
wait_status worker2 DEAD 20 && ok "worker2 marked DEAD after timeout (6s here)" || bad "worker2 DEAD" "still $(status_of worker2 status)"
[ "$(status_of worker2 dead_since)" != "None" ] && ok "dead_since set" || bad "dead_since" "None"
expect "other workers still ALIVE" "ALIVE ALIVE ALIVE" "$(status_of worker1 status) $(status_of worker3 status) $(status_of worker4 status)"
for _ in 1 2 3 4 5 6 7 8; do
  curl -s localhost:8000/api/v1/admin/placement-test | grep -q worker2 && { bad "placement excludes DEAD" "worker2 chosen"; break; }
done; ok "placement never picks DEAD worker2"

echo "--- recovery: restart worker2"
start_worker 2
wait_status worker2 ALIVE 15 && ok "worker2 back to ALIVE after restart" || bad "worker2 ALIVE" "still DEAD"
expect "dead_since cleared" None "$(status_of worker2 dead_since)"

echo "--- controller restart keeps state; grace period avoids false DEAD"
kill $CPID; wait $CPID 2>/dev/null
sleep 2
start_controller
for _ in $(seq 1 20); do curl -sf localhost:8000/health >/dev/null && break; sleep 0.5; done
expect "workers still known after controller restart" worker1 "$([ "$(status_of worker1 worker_id)" = worker1 ] && echo worker1)"
sleep 4
for i in 1 2 3 4; do expect "worker$i ALIVE after controller restart" ALIVE "$(status_of worker$i status)"; done

echo "--- data plane (upload, metadata, patch, list, download, delete)"
for i in 1 2 3 4; do
  curl -sf -X POST localhost:8000/internal/workers/register \
    -H 'Content-Type: application/json' \
    -d "{\"worker_id\":\"worker$i\",\"address\":\"http://127.0.0.1:800$i\"}" >/dev/null
done
head -c 1572864 /dev/urandom > "$T/sample.bin"
up=$(curl -s -w '\n%{http_code}' -X POST localhost:8000/api/v1/objects \
  -F "file=@$T/sample.bin" -F 'metadata={"env":"smoke"}')
up_code=$(echo "$up" | tail -n1)
up_body=$(echo "$up" | sed '$d')
expect "POST /api/v1/objects (1.5 MiB) -> 201" 201 "$up_code"
oid=$(echo "$up_body" | $PY -c 'import sys,json; print(json.load(sys.stdin)["object_id"])')
cc=$(echo "$up_body" | $PY -c 'import sys,json; print(json.load(sys.stdin)["chunk_count"])')
expect "chunk_count is 2" 2 "$cc"

total=$(curl -s localhost:8000/api/v1/objects | $PY -c 'import sys,json; print(json.load(sys.stdin)["total"])')
expect "GET /api/v1/objects total=1" 1 "$total"

meta_env=$(curl -s "localhost:8000/api/v1/objects/$oid/metadata" | $PY -c 'import sys,json; print(json.load(sys.stdin)["custom"]["env"])')
expect "GET metadata custom.env=smoke" smoke "$meta_env"

patched=$(curl -s -X PATCH "localhost:8000/api/v1/objects/$oid/metadata" \
  -H 'Content-Type: application/json' -d '{"env":"prod","note":"hi"}')
patch_ok=$(echo "$patched" | $PY -c 'import sys,json; c=json.load(sys.stdin)["custom"]; print(c["env"], c["note"])')
expect "PATCH metadata updates custom" "prod hi" "$patch_ok"

curl -sf "localhost:8000/api/v1/objects/$oid" -o "$T/downloaded.bin"
cmp -s "$T/sample.bin" "$T/downloaded.bin" && ok "GET /api/v1/objects/{id} matches uploaded bytes" || bad "download mismatch" "files differ"

chunks_on_disk=$(find "$T"/w{1,2,3,4} -name '*.chunk' | wc -l | tr -d ' ')
expect "4 chunk replicas stored across workers (2 chunks * R=2)" 4 "$chunks_on_disk"

del_code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "localhost:8000/api/v1/objects/$oid")
expect "DELETE /api/v1/objects/{id} -> 204" 204 "$del_code"
expect "GET after DELETE -> 404" 404 "$(curl -s -o /dev/null -w '%{http_code}' "localhost:8000/api/v1/objects/$oid")"
chunks_after=$(find "$T"/w{1,2,3,4} -name '*.chunk' | wc -l | tr -d ' ')
expect "worker chunk files cleaned up after DELETE" 0 "$chunks_after"

: > "$T/empty.bin"
z_code=$(curl -s -w '\n%{http_code}' -X POST localhost:8000/api/v1/objects -F "file=@$T/empty.bin")
z_http=$(echo "$z_code" | tail -n1)
z_id=$(echo "$z_code" | sed '$d' | $PY -c 'import sys,json; print(json.load(sys.stdin)["object_id"])')
expect "0-byte upload -> 201" 201 "$z_http"
expect "0-byte download -> 200" 200 "$(curl -s -o "$T/z.out" -w '%{http_code}' "localhost:8000/api/v1/objects/$z_id")"
[ ! -s "$T/z.out" ] && ok "0-byte download body is empty" || bad "0-byte body" "non-empty"
expect "0-byte delete -> 204" 204 "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "localhost:8000/api/v1/objects/$z_id")"

echo "--- controller log (transitions only)"
grep -E 'ALIVE -> DEAD|DEAD -> ALIVE|registered' $T/controller.log | tail -n 12 || true

echo; echo "passed=$pass failed=$fail"
[ "$fail" = 0 ]
