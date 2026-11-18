#!/usr/bin/env bash
# Demo 5: Worker Failure, Read Continuity, Grace-Period Re-Replication & Rejoin Reconciliation.
source "$(dirname "$0")/lib.sh"

banner "DEMO 5: Worker Failure, Re-Replication & Rejoin"
what_this_proves "Downloads survive worker loss immediately, chunks re-replicate onto survivors after grace period, and rejoining workers trim back to R=2."

ensure_cluster_ready

SRC_FILE="${ROOT_DIR}/demo/files/large_15mb.bin"
ORIG_SHA="$(sha256_file "$SRC_FILE")"

step "1. BEFORE — Uploading large_15mb.bin (15 MiB) and verifying 4 ALIVE workers"
UP_JSON="$(cli upload "$SRC_FILE" --meta demo=failure --json)"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UP_JSON")"
register_temp_object "$OID"
echo "Uploaded object_id=${OID} sha256=${ORIG_SHA}"
run_cmd cli workers
run_cmd cli replication

pause_step

step "2. DURING — Stopping worker2 and verifying immediate read continuity"
stop_worker_node worker2
wait_until 45 "worker2 marked DEAD" worker_is_status worker2 DEAD
run_cmd cli workers
run_cmd cli replication

DL_DURING="${DEMO_TMP_DIR}/dl_during.bin"
run_cmd cli download "$OID" -o "$DL_DURING"
DL_SHA="$(sha256_file "$DL_DURING")"
[ "$DL_SHA" = "$ORIG_SHA" ] || fail_msg "Download SHA-256 mismatch during outage!"

pause_step

step "3. REPAIR — Waiting past REPAIR_GRACE_PERIOD for surviving workers to reach R=2"
wait_until 90 "all chunks back to 2 valid replicas on surviving workers" "$PY" -c "
import json, sys, urllib.request
with urllib.request.urlopen('${BASE_URL}/api/v1/admin/objects/${OID}/chunks', timeout=3) as r:
    chunks = json.load(r)['chunks']
for c in chunks:
    ok = [x['worker_id'] for x in c['replicas'] if x['state'] == 'OK' and x['worker_status'] == 'ALIVE']
    if len(ok) != 2 or 'worker2' in ok:
        sys.exit(1)
sys.exit(0)
"
run_cmd cli replication
run_cmd cli chunks "$OID"

pause_step

step "4. AFTER — Restarting worker2 and waiting for rejoin reconciliation (2 replicas, 0 over-replicated)"
start_worker_node worker2
wait_until 45 "worker2 ALIVE" worker_is_status worker2 ALIVE
wait_until 60 "cluster reconciled to 2 replicas with 0 over-replicated" "$PY" -c "
import json, sys, urllib.request
with urllib.request.urlopen('${BASE_URL}/api/v1/admin/replication', timeout=3) as r:
    rep = json.load(r)
if rep['under_replicated'] != 0 or rep['over_replicated'] != 0 or rep['healthy'] != rep['total_chunks']:
    sys.exit(1)
sys.exit(0)
"
run_cmd cli workers
run_cmd cli replication
run_cmd cli chunks "$OID"

DL_AFTER="${DEMO_TMP_DIR}/dl_after.bin"
run_cmd cli download "$OID" -o "$DL_AFTER"
[ "$(sha256_file "$DL_AFTER")" = "$ORIG_SHA" ] || fail_msg "Final download SHA-256 mismatch!"

pass_msg "Demo 5 completed successfully."
