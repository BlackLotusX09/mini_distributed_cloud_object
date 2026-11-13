#!/usr/bin/env bash
# End-to-end checks of the worker hardening checklist against the running compose stack.
# Usage: docker compose up --build -d && bash scripts/docker_verify.sh
cd "$(dirname "$0")/.." || exit 1
pass=0; fail=0
ok()  { echo "PASS  $1"; pass=$((pass+1)); }
bad() { echo "FAIL  $1  ($2)"; fail=$((fail+1)); }
expect() { [ "$2" = "$3" ] && ok "$1" || bad "$1" "expected $2, got $3"; }
code() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@"; }
W1=http://localhost:8001; W2=http://localhost:8002; W3=http://localhost:8003

wait_healthy() {  # all workers healthy and controller sees 4 active
  for _ in $(seq 1 60); do
    if bash scripts/check_health.sh >/dev/null 2>&1 \
       && curl -s localhost:8000/health | grep -Eq '"active_workers": ?4'; then return 0; fi
    sleep 1
  done
  return 1
}

T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
head -c 1048576 /dev/urandom > "$T/max.bin"
head -c 1048577 /dev/urandom > "$T/over.bin"
head -c 4096    /dev/urandom > "$T/small.bin"
head -c 200000  /dev/urandom > "$T/persist.bin"

wait_healthy && ok "all 5 containers healthy; controller sees 4 workers" || { bad "startup" "stack not healthy"; exit 1; }

echo "--- registration"
addrs=$(curl -s localhost:8000/api/v1/admin/workers)
for i in 1 2 3 4; do
  echo "$addrs" | grep -Eq "\"address\": ?\"http://worker$i:8000\"" \
    && ok "worker$i advertises http://worker$i:8000" || bad "worker$i address" "$addrs"
done
echo "$addrs" | grep -Eq 'localhost|127\.0\.0\.1' && bad "no localhost in addresses" "$addrs" || ok "no localhost/127.0.0.1 advertised"

echo "--- boundary compliance"
expect "PUT exactly 1048576 bytes -> 201" 201 "$(code -X PUT --data-binary @$T/max.bin $W1/internal/chunks/max1)"
expect "PUT 1048577 bytes -> 400" 400 "$(code -X PUT --data-binary @$T/over.bin $W1/internal/chunks/over1)"
curl -s -X PUT --data-binary @$T/over.bin $W1/internal/chunks/over1 | grep -q CHUNK_TOO_LARGE && ok "error code CHUNK_TOO_LARGE" || bad "error code" "missing"
expect "oversized chunk not stored -> 404" 404 "$(code $W1/internal/chunks/over1)"
curl -s $W1/internal/chunks/max1 | cmp -s - "$T/max.bin" && ok "1 MiB chunk round-trips byte-exact" || bad "1MiB roundtrip" "differs"

echo "--- path traversal / invalid ids"
expect "../ traversal -> 400" 400 "$(code --path-as-is $W1/internal/chunks/../etc/passwd)"
expect "encoded traversal -> 400" 400 "$(code --path-as-is $W1/internal/chunks/%2e%2e%2fsecret)"
expect "special chars -> 400" 400 "$(code --path-as-is "$W1/internal/chunks/a.b")"
expect "129-char id -> 400" 400 "$(code -X PUT --data-binary hi $W1/internal/chunks/$(printf 'x%.0s' $(seq 1 129)))"
curl -s --path-as-is $W1/internal/chunks/../x | grep -q INVALID_CHUNK_ID && ok "error code INVALID_CHUNK_ID" || bad "error code" "missing"

echo "--- API semantics"
sum=$(shasum -a 256 "$T/small.bin" | cut -d' ' -f1)
expect "PUT new -> 201" 201 "$(code -X PUT -H "X-Chunk-Checksum: $sum" --data-binary @$T/small.bin $W1/internal/chunks/sem1)"
expect "PUT identical again -> 200" 200 "$(code -X PUT --data-binary @$T/small.bin $W1/internal/chunks/sem1)"
expect "PUT different data same id -> 409" 409 "$(code -X PUT --data-binary other $W1/internal/chunks/sem1)"
expect "PUT wrong checksum -> 400" 400 "$(code -X PUT -H 'X-Chunk-Checksum: deadbeef' --data-binary @$T/small.bin $W1/internal/chunks/sem2)"
hdr=$(curl -s -D - -o /dev/null $W1/internal/chunks/sem1 | tr -d '\r' | grep -i '^x-chunk-checksum' | awk '{print $2}')
expect "GET returns X-Chunk-Checksum header" "$sum" "$hdr"
expect "DELETE -> 204" 204 "$(code -X DELETE $W1/internal/chunks/sem1)"
expect "DELETE again (idempotent) -> 204" 204 "$(code -X DELETE $W1/internal/chunks/sem1)"
expect "GET deleted -> 404" 404 "$(code $W1/internal/chunks/sem1)"
expect "list for foreign worker id -> 404" 404 "$(code $W1/internal/workers/worker2/chunks)"
curl -s $W1/internal/workers/worker1/chunks | grep -q '"max1"' && ok "list shows stored chunk" || bad "list" "max1 missing"

echo "--- checksum enforceability (bit flip on disk)"
curl -s -o /dev/null -X PUT --data-binary @$T/small.bin $W1/internal/chunks/flipme
docker exec worker1 python -c "
p='/data/chunks/fl/flipme.chunk'
b=bytearray(open(p,'rb').read()); b[100]^=1; open(p,'wb').write(bytes(b))"
expect "GET of bit-flipped chunk -> 409" 409 "$(code $W1/internal/chunks/flipme)"
body=$(curl -s $W1/internal/chunks/flipme)
echo "$body" | grep -q CHUNK_CORRUPT && ok "error code CHUNK_CORRUPT, JSON body (no chunk bytes)" || bad "corrupt body" "$body"

echo "--- volume isolation"
curl -s -o /dev/null -X PUT --data-binary @$T/small.bin $W1/internal/chunks/iso1
expect "chunk on worker1 absent on worker2 -> 404" 404 "$(code $W2/internal/chunks/iso1)"
expect "worker2 container has no such file" "" "$(docker exec worker2 sh -c 'ls /data/chunks/is 2>/dev/null')"

echo "--- restart / SIGKILL / compose down (no -v)"
curl -s -o /dev/null -X PUT --data-binary @$T/persist.bin $W3/internal/chunks/persist1
same() { curl -s $W3/internal/chunks/persist1 | cmp -s - "$T/persist.bin"; }
docker restart worker3 >/dev/null && wait_healthy && same && ok "survives docker restart" || bad "docker restart" "chunk lost/changed"
docker kill worker3 >/dev/null && docker start worker3 >/dev/null && wait_healthy && same \
  && ok "survives SIGKILL (docker kill) + start" || bad "docker kill" "chunk lost/changed"
docker compose down >/dev/null 2>&1
docker volume ls -q | grep -q 'worker3-data' && ok "named volumes survive compose down" || bad "volumes" "worker3-data gone"
docker compose up -d >/dev/null 2>&1
wait_healthy && ok "stack healthy again after down/up; workers re-registered" || bad "down/up" "not healthy"
same && ok "chunk intact after compose down/up" || bad "down/up persistence" "chunk lost/changed"
curl -s $W3/internal/workers/worker3/chunks | grep -q persist1 && ok "catalog (list) intact after down/up" || bad "catalog" "persist1 missing"
cur=$(curl -s $W1/internal/chunks/flipme -o /dev/null -w '%{http_code}')
expect "corrupt chunk still refused after restart" 409 "$cur"

echo "--- stopping one worker"
docker stop worker2 >/dev/null
expect "stopped worker2 unreachable" 000 "$(code $W2/health)"
expect "worker1 still healthy" 200 "$(code $W1/health)"
docker start worker2 >/dev/null; wait_healthy && ok "worker2 rejoins after start" || bad "worker2 rejoin" "not healthy"

echo; echo "passed=$pass failed=$fail"
[ "$fail" = 0 ]

