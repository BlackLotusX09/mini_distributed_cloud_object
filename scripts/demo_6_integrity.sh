#!/usr/bin/env bash
# Demo 6: End-to-End Data Integrity — Silent Bit-Rot Detection, Scrubbing & Self-Healing.
source "$(dirname "$0")/lib.sh"

INCLUDE_UNRECOVERABLE=0
for _arg in "$@"; do
  if [ "$_arg" = "--include-unrecoverable" ]; then
    INCLUDE_UNRECOVERABLE=1
  fi
done

banner "DEMO 6: Two-Layer Data Integrity, Scrubbing & Self-Healing"
what_this_proves "Silent disk corruption never reaches clients; reads fall back transparently and scrub+repair heal back to 2 valid replicas."

ensure_cluster_ready

SRC_FILE="${ROOT_DIR}/demo/files/medium_6mb.bin"
ORIG_SHA="$(sha256_file "$SRC_FILE")"

step "1. Uploading multi-chunk object and printing placement"
UP_JSON="$(cli upload "$SRC_FILE" --meta demo=integrity --json)"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UP_JSON")"
register_temp_object "$OID"
run_cmd cli chunks "$OID"

CHUNKS_JSON="$(cli chunks "$OID" --json)"
read -r VICTIM_CID VICTIM_WORKER <<<"$("$PY" -c "
import json, sys
ch0 = json.loads(sys.argv[1])['chunks'][0]
print(ch0['chunk_id'], ch0['replicas'][0]['worker_id'])
" "$CHUNKS_JSON")"

pause_step

step "2. Injecting silent bit-rot on 1 replica of chunk 0 (${VICTIM_WORKER}: ${VICTIM_CID})"
run_cmd bash "${ROOT_DIR}/scripts/corrupt_chunk.sh" "$VICTIM_WORKER" "$VICTIM_CID"

step "3. Immediately downloading object and verifying intact SHA-256 continuity"
DL_1="${DEMO_TMP_DIR}/dl_corrupt1.bin"
run_cmd cli download "$OID" -o "$DL_1"
DL1_SHA="$(sha256_file "$DL_1")"
[ "$DL1_SHA" = "$ORIG_SHA" ] || fail_msg "Download returned corrupt data!"
echo "SHA-256 MATCH (${DL1_SHA})"

pause_step

step "4. Triggering scrub and waiting for repair to replace corrupt replica on another worker"
run_cmd cli scrub
wait_until 60 "chunk 0 healed to 2 OK replicas excluding ${VICTIM_WORKER}" "$PY" -c "
import json, sys, urllib.request
with urllib.request.urlopen('${BASE_URL}/api/v1/admin/objects/${OID}/chunks', timeout=3) as r:
    ch0 = json.load(r)['chunks'][0]
ok = [x['worker_id'] for x in ch0['replicas'] if x['state'] == 'OK' and x['worker_status'] == 'ALIVE']
if len(ok) == 2 and '${VICTIM_WORKER}' not in ok:
    sys.exit(0)
sys.exit(1)
"
wait_until 30 "corrupt chunk removed from ${VICTIM_WORKER} disk" "$PY" -c "
import json, sys
disk = {x['chunk_id'] for x in json.loads(sys.argv[1])}
sys.exit(0 if '${VICTIM_CID}' not in disk else 1)
" "$(worker_disk_chunks "$VICTIM_WORKER")"

run_cmd cli replication
run_cmd cli chunks "$OID"

if [ "$INCLUDE_UNRECOVERABLE" = "1" ]; then
  pause_step
  step "5. [--include-unrecoverable] Corrupting BOTH replicas of chunk 0 of a test object"
  U_SRC="${ROOT_DIR}/demo/files/sample.txt"
  U_JSON="$(cli upload "$U_SRC" --meta demo=unrecoverable --json)"
  U_OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$U_JSON")"
  register_temp_object "$U_OID"
  U_CHUNKS="$(cli chunks "$U_OID" --json)"
  read -r U_CID U_W1 U_W2 <<<"$("$PY" -c "
import json, sys
ch0 = json.loads(sys.argv[1])['chunks'][0]
print(ch0['chunk_id'], ch0['replicas'][0]['worker_id'], ch0['replicas'][1]['worker_id'])
" "$U_CHUNKS")"
  run_cmd bash "${ROOT_DIR}/scripts/corrupt_chunk.sh" "$U_W1" "$U_CID"
  run_cmd bash "${ROOT_DIR}/scripts/corrupt_chunk.sh" "$U_W2" "$U_CID"
  RESP_CODE="$("$PY" -c "
import json, sys, urllib.error, urllib.request
try:
    urllib.request.urlopen('${BASE_URL}/api/v1/objects/${U_OID}', timeout=10)
    sys.exit(1)
except urllib.error.HTTPError as e:
    body = json.loads(e.read().decode())
    assert e.code in (500, 503), f'Unexpected status {e.code}'
    assert body['error'] == 'DATA_CORRUPT', f'Unexpected error body: {body}'
    print(f'HTTP {e.code} {body}')
" )"
  echo "Verified fail-stop on dual corruption: ${RESP_CODE}"
fi

pass_msg "Demo 6 completed successfully."
