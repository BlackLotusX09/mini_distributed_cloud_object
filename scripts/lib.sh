#!/usr/bin/env bash
# Shared helper library for scripts/demo_1_*.sh .. scripts/demo_6_*.sh.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${ROOT_DIR}/.venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="$(command -v python3)"
fi
CLI="${ROOT_DIR}/client/objstore.py"
BASE_URL="${OBJSTORE_URL:-${CONTROLLER_URL:-http://localhost:8000}}"
export OBJSTORE_URL="$BASE_URL"

NON_INTERACTIVE="${DEMO_NON_INTERACTIVE:-0}"
for _arg in "$@"; do
  if [ "$_arg" = "--non-interactive" ]; then
    NON_INTERACTIVE=1
  fi
done

if [ -t 1 ]; then
  C_BLUE='\033[1;34m'
  C_CYAN='\033[1;36m'
  C_GREEN='\033[1;32m'
  C_YELLOW='\033[1;33m'
  C_RED='\033[1;31m'
  C_BOLD='\033[1m'
  C_RESET='\033[0m'
else
  C_BLUE='' C_CYAN='' C_GREEN='' C_YELLOW='' C_RED='' C_BOLD='' C_RESET=''
fi

banner() {
  echo -e "\n${C_BLUE}======================================================================${C_RESET}"
  echo -e "${C_BOLD}$1${C_RESET}"
  echo -e "${C_BLUE}======================================================================${C_RESET}"
}

step() {
  echo -e "\n${C_CYAN}>>> $1${C_RESET}"
}

what_this_proves() {
  echo -e "${C_YELLOW}WHAT THIS PROVES:${C_RESET} $1"
}

pass_msg() {
  echo -e "${C_GREEN}PASS:${C_RESET} $1"
}

fail_msg() {
  echo -e "${C_RED}FAIL:${C_RESET} $1" >&2
  exit 1
}

run_cmd() {
  echo -e "${C_BOLD}\$ $*${C_RESET}"
  "$@"
}

pause_step() {
  if [ "$NON_INTERACTIVE" = "1" ] || [ ! -t 0 ]; then
    return 0
  fi
  read -r -p "Press Enter to continue..." _
}

wait_until() {
  local timeout_sec="$1"
  local desc="$2"
  shift 2
  local deadline=$(( $(date +%s) + timeout_sec ))
  while true; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      fail_msg "Timed out after ${timeout_sec}s waiting for: ${desc}"
    fi
    sleep 0.5
  done
}

cli() {
  "$PY" "$CLI" --url "$BASE_URL" "$@"
}

sha256_file() {
  "$PY" -c "
import hashlib, sys
h = hashlib.sha256()
with open(sys.argv[1], 'rb') as f:
    for blk in iter(lambda: f.read(1048576), b''):
        h.update(blk)
print(h.hexdigest())
" "$1"
}

# ---- Cluster mode detection & local fallback management ----
DEMO_TMP_DIR="$(mktemp -d)"
declare -a DEMO_CREATED_OBJECTS=()
declare -a DEMO_STOPPED_WORKERS=()
OWNED_LOCAL_CLUSTER=0
DEMO_MODE=""
LOCAL_CLUSTER_ROOT="${LOCAL_CLUSTER_ROOT:-}"
LOCAL_DB_PATH="${LOCAL_DB_PATH:-}"

detect_demo_mode() {
  if [ -n "${DFS_SHARED_LOCAL:-}" ] && [ -n "$LOCAL_CLUSTER_ROOT" ]; then
    DEMO_MODE="local"
    export WORKER_DATA_ROOT="$LOCAL_CLUSTER_ROOT"
    return 0
  fi
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 && docker inspect controller >/dev/null 2>&1; then
    DEMO_MODE="docker"
    return 0
  fi
  DEMO_MODE="local"
}

_start_local_worker() {
  local w="$1"
  local idx="${w#worker}"
  local log_file="${LOCAL_CLUSTER_ROOT}/${w}.log"
  local offset=0
  if [ -f "$log_file" ]; then
    offset="$(wc -c < "$log_file" | tr -d ' ')"
  fi
  (
    cd "${ROOT_DIR}/worker"
    WORKER_ID="$w" PORT="800${idx}" DATA_DIR="${LOCAL_CLUSTER_ROOT}/${w}" \
      CONTROLLER_URL="$BASE_URL" HEARTBEAT_INTERVAL="1s" WORKER_SCRUB_YIELD_DELAY="0" \
      exec "$PY" app.py >>"$log_file" 2>&1
  ) &
  echo "$!" > "${LOCAL_CLUSTER_ROOT}/${w}.pid"

  # Wait until worker registered itself and health is up, then point address to 127.0.0.1
  wait_until 20 "${w} registered & listening" "$PY" -c "
import os, sys, urllib.request
with open(sys.argv[1], 'rb') as f:
    f.seek(int(sys.argv[2]))
    assert b'registered as' in f.read()
assert urllib.request.urlopen(sys.argv[3], timeout=2).status == 200
" "$log_file" "$offset" "http://127.0.0.1:800${idx}/health"

  "$PY" -c "
import json, urllib.request
req = urllib.request.Request(
    '${BASE_URL}/internal/workers/register',
    data=json.dumps({'worker_id': '${w}', 'address': 'http://127.0.0.1:800${idx}'}).encode(),
    headers={'Content-Type': 'application/json'},
    method='POST',
)
urllib.request.urlopen(req, timeout=3).read()
"
}

_stop_local_worker() {
  local w="$1"
  local pf="${LOCAL_CLUSTER_ROOT}/${w}.pid"
  if [ -f "$pf" ]; then
    local pid
    pid="$(cat "$pf")"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$pf"
  fi
}

start_local_cluster() {
  LOCAL_CLUSTER_ROOT="${DEMO_TMP_DIR}/cluster"
  LOCAL_DB_PATH="${LOCAL_CLUSTER_ROOT}/controller.db"
  export LOCAL_CLUSTER_ROOT LOCAL_DB_PATH WORKER_DATA_ROOT="$LOCAL_CLUSTER_ROOT"
  mkdir -p "$LOCAL_CLUSTER_ROOT"
  OWNED_LOCAL_CLUSTER=1

  local grace="${DEMO_REPAIR_GRACE_PERIOD:-8}"
  DB_PATH="$LOCAL_DB_PATH" HEARTBEAT_TIMEOUT=6 LIVENESS_CHECK_INTERVAL=1 \
    REPAIR_GRACE_PERIOD="$grace" REPAIR_INTERVAL=2 SCRUB_INTERVAL=300 \
    "$PY" -m uvicorn --app-dir "${ROOT_DIR}/controller" main:app --port 8000 --log-level warning \
    >"${LOCAL_CLUSTER_ROOT}/controller.log" 2>&1 &
  echo "$!" > "${LOCAL_CLUSTER_ROOT}/controller.pid"

  wait_until 20 "controller /health" curl -sf "${BASE_URL}/health"

  for w in worker1 worker2 worker3 worker4; do
    _start_local_worker "$w"
  done
}

ensure_cluster_ready() {
  "$PY" "${ROOT_DIR}/scripts/ensure_demo_files.py" --quiet
  detect_demo_mode
  if ! curl -sf --max-time 2 "${BASE_URL}/health" >/dev/null 2>&1; then
    if [ "$DEMO_MODE" = "docker" ]; then
      fail_msg "Controller not reachable at ${BASE_URL}; start Docker stack with: docker compose up -d --build"
    fi
    step "Starting local 1-controller + 4-worker cluster for demo (Docker unavailable)"
    start_local_cluster
  fi
  wait_until 30 "4 ALIVE workers" "$PY" -c "
import json, sys, urllib.request
with urllib.request.urlopen('${BASE_URL}/api/v1/admin/workers', timeout=3) as r:
    ws = json.load(r)
alive = [w for w in ws if w['status'] == 'ALIVE']
sys.exit(0 if len(alive) >= 4 else 1)
"
}

register_temp_object() {
  DEMO_CREATED_OBJECTS+=("$1")
}

stop_worker_node() {
  local w="$1"
  DEMO_STOPPED_WORKERS+=("$w")
  if [ "$DEMO_MODE" = "docker" ]; then
    run_cmd docker stop "$w" >/dev/null
  else
    echo "\$ stop_worker_node $w"
    _stop_local_worker "$w"
  fi
}

start_worker_node() {
  local w="$1"
  if [ "$DEMO_MODE" = "docker" ]; then
    run_cmd docker start "$w" >/dev/null
  else
    echo "\$ start_worker_node $w"
    _start_local_worker "$w"
  fi
}

worker_is_status() {
  local w="$1"
  local want="$2"
  "$PY" -c "
import json, sys, urllib.request
with urllib.request.urlopen('${BASE_URL}/api/v1/admin/workers', timeout=3) as r:
    ws = {x['worker_id']: x for x in json.load(r)}
sys.exit(0 if ws.get('${w}', {}).get('status') == '${want}' else 1)
"
}

worker_disk_chunks() {
  local w="$1"
  if [ "$DEMO_MODE" = "docker" ]; then
    docker exec "$w" python3 -c "
import json, os
out = []
for dp, _, fs in os.walk('/data/chunks'):
    for f in fs:
        if f.endswith('.chunk') and not f.startswith('.'):
            p = os.path.join(dp, f)
            out.append({'chunk_id': f[:-6], 'mtime': os.path.getmtime(p)})
print(json.dumps(sorted(out, key=lambda x: x['chunk_id'])))
"
  else
    "$PY" -c "
import json, os, sys
root = os.path.join(sys.argv[1], sys.argv[2], 'chunks')
out = []
for dp, _, fs in os.walk(root):
    for f in fs:
        if f.endswith('.chunk') and not f.startswith('.'):
            p = os.path.join(dp, f)
            out.append({'chunk_id': f[:-6], 'mtime': os.path.getmtime(p)})
print(json.dumps(sorted(out, key=lambda x: x['chunk_id'])))
" "$LOCAL_CLUSTER_ROOT" "$w"
  fi
}

worker_put_log_count() {
  local count=0
  if [ "$DEMO_MODE" = "docker" ]; then
    for w in worker1 worker2 worker3 worker4; do
      local n
      n="$(docker logs "$w" 2>&1 | grep -c 'PUT /internal/chunks/' || true)"
      count=$((count + n))
    done
  else
    for w in worker1 worker2 worker3 worker4; do
      local lf="${LOCAL_CLUSTER_ROOT}/${w}.log"
      if [ -f "$lf" ]; then
        local n
        n="$(grep -c 'PUT /internal/chunks/' "$lf" || true)"
        count=$((count + n))
      fi
    done
  fi
  echo "$count"
}

run_invariant_check() {
  if [ "$DEMO_MODE" = "docker" ]; then
    "$PY" "${ROOT_DIR}/scripts/check_invariants.py"
  else
    "$PY" "${ROOT_DIR}/scripts/check_invariants.py" --db "$LOCAL_DB_PATH" --data-root "$LOCAL_CLUSTER_ROOT"
  fi
}

demo_cleanup() {
  set +e
  if [ "${#DEMO_CREATED_OBJECTS[@]}" -gt 0 ]; then
    for oid in "${DEMO_CREATED_OBJECTS[@]}"; do
      if [ -n "$oid" ]; then
        "$PY" "$CLI" --url "$BASE_URL" delete "$oid" >/dev/null 2>&1 || true
      fi
    done
  fi
  if [ "${#DEMO_STOPPED_WORKERS[@]}" -gt 0 ]; then
    if [ "$DEMO_MODE" = "docker" ]; then
      docker start worker1 worker2 worker3 worker4 >/dev/null 2>&1 || true
    elif [ "$OWNED_LOCAL_CLUSTER" != "1" ] && [ -n "${LOCAL_CLUSTER_ROOT:-}" ] && [ -f "${LOCAL_CLUSTER_ROOT}/controller.pid" ]; then
      for w in "${DEMO_STOPPED_WORKERS[@]}"; do
        if [ ! -f "${LOCAL_CLUSTER_ROOT}/${w}.pid" ]; then
          _start_local_worker "$w" >/dev/null 2>&1 || true
        fi
      done
    fi
  fi
  if [ "$OWNED_LOCAL_CLUSTER" = "1" ] && [ -n "${LOCAL_CLUSTER_ROOT:-}" ]; then
    for name in controller worker1 worker2 worker3 worker4; do
      _stop_local_worker "$name"
    done
  fi
  rm -rf "$DEMO_TMP_DIR" 2>/dev/null || true
}

trap demo_cleanup EXIT INT TERM
