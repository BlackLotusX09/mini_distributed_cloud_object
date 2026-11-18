#!/usr/bin/env bash
# Demo 2: Fixed-Size 1 MiB Chunking & Balanced Distribution Across Workers.
source "$(dirname "$0")/lib.sh"

banner "DEMO 2: Chunking (1 MiB Stripes) & Worker Distribution"
what_this_proves "Files > 1 MiB are split into 1,048,576-byte chunks with a smaller partial tail chunk distributed across workers."

ensure_cluster_ready

SRC_FILE="${ROOT_DIR}/demo/files/medium_6mb.bin"

step "1. Uploading medium_6mb.bin (6,500,000 bytes -> expected 7 chunks)"
UP_JSON="$(cli upload "$SRC_FILE" --meta demo=chunking --json)"
OID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$UP_JSON")"
register_temp_object "$OID"
run_cmd cli info "$OID"

pause_step

step "2. Inspecting chunk map & asserting chunk sizes"
run_cmd cli chunks "$OID"
CHUNKS_JSON="$(cli chunks "$OID" --json)"
"$PY" -c "
import json, sys
data = json.loads(sys.argv[1])
chunks = data['chunks']
assert len(chunks) == 7, f'Expected 7 chunks, got {len(chunks)}'
for i in range(6):
    assert chunks[i]['size'] == 1_048_576, f'Chunk {i} size {chunks[i][\"size\"]} != 1048576'
tail = chunks[-1]['size']
assert 0 < tail < 1_048_576, f'Tail chunk size {tail} not strictly < 1 MiB'
print(f'Verified {len(chunks)} chunks: chunks[0..5]=1048576 B, tail={tail} B (< 1 MiB)')
" "$CHUNKS_JSON"

step "3. Worker chunk distribution across cluster"
run_cmd cli workers
pass_msg "Demo 2 completed successfully."
