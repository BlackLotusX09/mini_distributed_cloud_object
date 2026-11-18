#!/usr/bin/env bash
# Automated end-to-end integrity & self-healing demo:
# Upload multi-chunk file -> corrupt 1 replica -> verify read continuity ->
# run scrub -> observe CORRUPT/re-replication -> verify restored 2-replica placement & download.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1
PY="${PWD}/.venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="$(command -v python3)"
fi
BASE_URL="${CONTROLLER_URL:-http://localhost:8000}"

TMP_DIR="$(mktemp -d)"
declare -a PIDS=()
STARTED_LOCAL=0

cleanup() {
  if [ "$STARTED_LOCAL" = "1" ] && [ "${#PIDS[@]}" -gt 0 ]; then
    kill "${PIDS[@]}" 2>/dev/null || true
    wait "${PIDS[@]}" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if ! curl -sf --max-time 2 "${BASE_URL}/health" >/dev/null 2>&1; then
  echo "=== No running controller at ${BASE_URL}; starting local 1-controller + 4-worker cluster ==="
  STARTED_LOCAL=1
  export WORKER_DATA_ROOT="$TMP_DIR"
  DB_PATH="$TMP_DIR/controller.db" HEARTBEAT_TIMEOUT=10 LIVENESS_CHECK_INTERVAL=2 \
    REPAIR_INTERVAL=1 REPAIR_GRACE_PERIOD=2 SCRUB_INTERVAL=300 \
    "$PY" -m uvicorn --app-dir controller main:app --port 8000 --log-level warning >"$TMP_DIR/controller.log" 2>&1 &
  PIDS+=("$!")

  for i in 1 2 3 4; do
    (cd worker && WORKER_ID="worker${i}" PORT="800${i}" DATA_DIR="$TMP_DIR/worker${i}" \
      CONTROLLER_URL="$BASE_URL" HEARTBEAT_INTERVAL=2s exec "$PY" app.py >"$TMP_DIR/worker${i}.log" 2>&1) &
    PIDS+=("$!")
  done

  for _ in $(seq 1 30); do
    if curl -sf "${BASE_URL}/health" | grep -Eq '"active_workers": ?4'; then
      break
    fi
    sleep 0.5
  done
  # Ensure local loopback addresses when running outside Docker
  for i in 1 2 3 4; do
    curl -sf -X POST "${BASE_URL}/internal/workers/register" \
      -H 'Content-Type: application/json' \
      -d "{\"worker_id\":\"worker${i}\",\"address\":\"http://127.0.0.1:800${i}\"}" >/dev/null
  done
  sleep 0.5
fi

echo "=== Step 1: Upload multi-chunk file (1.5 MiB) ==="
head -c 1572864 /dev/urandom > "$TMP_DIR/sample.bin"
ORIG_SHA="$(shasum -a 256 "$TMP_DIR/sample.bin" | awk '{print $1}')"
UPLOAD_JSON="$(curl -sf -X POST "${BASE_URL}/api/v1/objects" -F "file=@$TMP_DIR/sample.bin")"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UPLOAD_JSON")"
echo "Uploaded object_id=${OID} sha256=${ORIG_SHA}"

echo ""
echo "=== Step 2: Initial chunk placement ==="
"$PY" scripts/print_replication.py --url "$BASE_URL" "$OID"

echo ""
echo "=== Step 3: Inject silent bit-rot on 1 replica of chunk 0 ==="
CHUNKS_JSON="$(curl -sf "${BASE_URL}/api/v1/admin/objects/${OID}/chunks")"
read -r VICTIM_CID VICTIM_WORKER <<<"$("$PY" -c "
import sys, json
d = json.loads(sys.argv[1])
ch0 = d['chunks'][0]
print(ch0['chunk_id'], ch0['replicas'][0]['worker_id'])
" "$CHUNKS_JSON")"
echo "Corrupting chunk=${VICTIM_CID} on worker=${VICTIM_WORKER}"
bash scripts/corrupt_chunk.sh "$VICTIM_WORKER" "$VICTIM_CID"

echo ""
echo "=== Step 4: Download object & verify SHA-256 continuity ==="
curl -sf "${BASE_URL}/api/v1/objects/${OID}" -o "$TMP_DIR/dl1.bin"
DL1_SHA="$(shasum -a 256 "$TMP_DIR/dl1.bin" | awk '{print $1}')"
echo "Download 1 sha256=${DL1_SHA}"
if [ "$ORIG_SHA" != "$DL1_SHA" ]; then
  echo "ERROR: Download checksum mismatch!" >&2
  exit 1
fi
echo "PASS: Read path delivered exact verified bytes despite corrupted replica."

echo ""
echo "=== Step 5: Trigger scrub & inspect admin summary ==="
SCRUB_OUT="$(curl -sf -X POST "${BASE_URL}/api/v1/admin/scrub")"
echo "Scrub result: ${SCRUB_OUT}"
curl -sf "${BASE_URL}/api/v1/admin/replication" | "$PY" -m json.tool

echo ""
echo "=== Step 6: Wait for background repair to restore 2 valid replicas ==="
for _ in $(seq 1 40); do
  HEALTH_JSON="$(curl -sf "${BASE_URL}/api/v1/admin/replication")"
  DONE="$("$PY" -c "
import sys, json
r = json.loads(sys.argv[1])
ok = (r['healthy'] == r['total_chunks'] and r['under_replicated'] == 0)
print('1' if ok else '0')
" "$HEALTH_JSON")"
  if [ "$DONE" = "1" ]; then
    break
  fi
  sleep 0.5
done

echo ""
echo "=== Step 7: Restored 2-replica placement ==="
"$PY" scripts/print_replication.py --url "$BASE_URL" "$OID"

echo ""
echo "=== Step 8: Final download verification ==="
curl -sf "${BASE_URL}/api/v1/objects/${OID}" -o "$TMP_DIR/dl2.bin"
DL2_SHA="$(shasum -a 256 "$TMP_DIR/dl2.bin" | awk '{print $1}')"
echo "Final download sha256=${DL2_SHA}"
if [ "$ORIG_SHA" != "$DL2_SHA" ]; then
  echo "ERROR: Final download checksum mismatch!" >&2
  exit 1
fi
echo "PASS: Demo integrity & self-healing completed successfully."
