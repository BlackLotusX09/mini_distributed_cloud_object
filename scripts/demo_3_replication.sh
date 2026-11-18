#!/usr/bin/env bash
# Demo 3: Replication Factor 2, Distinct Workers & On-Disk Worker Parity.
source "$(dirname "$0")/lib.sh"

banner "DEMO 3: Replication Factor 2 & Disk-to-DB Parity"
what_this_proves "Every chunk has exactly 2 replicas on 2 distinct workers in SQLite and matching .chunk files on worker disks."

ensure_cluster_ready

SRC_FILE="${ROOT_DIR}/demo/files/medium_6mb.bin"

step "1. Uploading multi-chunk object"
UP_JSON="$(cli upload "$SRC_FILE" --meta demo=replication --json)"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UP_JSON")"
register_temp_object "$OID"
run_cmd cli chunks "$OID"

pause_step

step "2. Gathering actual on-disk chunk listings from worker1..worker4"
W1_DISK="$(worker_disk_chunks worker1)"
W2_DISK="$(worker_disk_chunks worker2)"
W3_DISK="$(worker_disk_chunks worker3)"
W4_DISK="$(worker_disk_chunks worker4)"

CHUNKS_JSON="$(cli chunks "$OID" --json)"
"$PY" -c "
import json, sys
cmap = json.loads(sys.argv[1])['chunks']
disks = {
    'worker1': {x['chunk_id'] for x in json.loads(sys.argv[2])},
    'worker2': {x['chunk_id'] for x in json.loads(sys.argv[3])},
    'worker3': {x['chunk_id'] for x in json.loads(sys.argv[4])},
    'worker4': {x['chunk_id'] for x in json.loads(sys.argv[5])},
}
for ch in cmap:
    cid = ch['chunk_id']
    reps = ch['replicas']
    wids = [r['worker_id'] for r in reps]
    assert len(reps) == 2 and len(set(wids)) == 2, f'{cid}: expected 2 distinct workers, got {reps}'
    for r in reps:
        assert r['state'] == 'OK', f'{cid}: replica not OK: {r}'
        assert cid in disks[r['worker_id']], f'{cid} missing on {r[\"worker_id\"]} disk'
    actual = sorted(w for w, s in disks.items() if cid in s)
    assert actual == sorted(wids), f'{cid}: disk holders {actual} != DB {sorted(wids)}'
print(f'Verified all {len(cmap)} chunks have 2 distinct OK replicas matching worker disks 1:1.')
" "$CHUNKS_JSON" "$W1_DISK" "$W2_DISK" "$W3_DISK" "$W4_DISK"

pass_msg "Demo 3 completed successfully."
