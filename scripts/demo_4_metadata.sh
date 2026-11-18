#!/usr/bin/env bash
# Demo 4: Metadata-Only Operations (GET & Merge-PATCH Without Touching Workers).
source "$(dirname "$0")/lib.sh"

banner "DEMO 4: Metadata Operations (Zero Worker Data Traffic on PATCH)"
what_this_proves "Custom metadata updates (add, update, null-delete) modify SQLite only while leaving chunks and checksum untouched."

ensure_cluster_ready

SRC_FILE="${ROOT_DIR}/demo/files/medium_6mb.bin"

step "1. Uploading object with initial custom metadata (env=dev, owner=alice)"
UP_JSON="$(cli upload "$SRC_FILE" --meta env=dev --meta owner=alice --json)"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UP_JSON")"
register_temp_object "$OID"

BEFORE_META="$(cli info "$OID" --json)"
BEFORE_CHUNKS="$(cli chunks "$OID" --json)"
BEFORE_WORKERS="$(cli workers --json)"
BEFORE_PUTS="$(worker_put_log_count)"
W_SNAP_BEFORE="$("$PY" -c "
import json, sys
print(json.dumps({w: json.loads(d) for w, d in zip(['worker1','worker2','worker3','worker4'], sys.argv[1:])}, sort_keys=True))
" "$(worker_disk_chunks worker1)" "$(worker_disk_chunks worker2)" "$(worker_disk_chunks worker3)" "$(worker_disk_chunks worker4)")"

run_cmd cli info "$OID"

pause_step

step "2. Merge-patching metadata: env=prod, tier=gold, owner=null"
sleep 0.05
run_cmd cli patch "$OID" env=prod tier=gold owner=null

AFTER_META="$(cli info "$OID" --json)"
AFTER_CHUNKS="$(cli chunks "$OID" --json)"
AFTER_WORKERS="$(cli workers --json)"
AFTER_PUTS="$(worker_put_log_count)"
W_SNAP_AFTER="$("$PY" -c "
import json, sys
print(json.dumps({w: json.loads(d) for w, d in zip(['worker1','worker2','worker3','worker4'], sys.argv[1:])}, sort_keys=True))
" "$(worker_disk_chunks worker1)" "$(worker_disk_chunks worker2)" "$(worker_disk_chunks worker3)" "$(worker_disk_chunks worker4)")"

"$PY" -c "
import json, sys
b_meta, a_meta = json.loads(sys.argv[1]), json.loads(sys.argv[2])
b_chk, a_chk = json.loads(sys.argv[3]), json.loads(sys.argv[4])
b_put, a_put = int(sys.argv[5]), int(sys.argv[6])
b_disk, a_disk = sys.argv[7], sys.argv[8]

assert a_meta['custom'] == {'env': 'prod', 'tier': 'gold'}, f'unexpected custom: {a_meta[\"custom\"]}'
assert a_meta['created_at'] == b_meta['created_at'], 'created_at changed!'
assert a_meta['modified_at'] > b_meta['modified_at'], 'modified_at did not advance!'
assert a_meta['checksum'] == b_meta['checksum'], 'checksum changed!'
assert a_chk['chunks'] == b_chk['chunks'], 'chunk map changed!'
assert a_put == b_put, f'worker PUT log count changed ({b_put} -> {a_put})'
assert b_disk == a_disk, 'worker chunk files or mtimes changed during PATCH!'
print('Verified: modified_at updated; created_at, checksum, chunk map, and worker disks completely untouched (0 PUTs).')
" "$BEFORE_META" "$AFTER_META" "$BEFORE_CHUNKS" "$AFTER_CHUNKS" "$BEFORE_PUTS" "$AFTER_PUTS" "$W_SNAP_BEFORE" "$W_SNAP_AFTER"

pass_msg "Demo 4 completed successfully."
