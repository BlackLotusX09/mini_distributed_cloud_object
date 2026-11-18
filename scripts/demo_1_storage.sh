#!/usr/bin/env bash
# Demo 1: Basic Object Storage — Upload, Metadata, Streaming Download & Byte-Exact Verification.
source "$(dirname "$0")/lib.sh"

banner "DEMO 1: Object Storage (Text & Binary Round-Trip)"
what_this_proves "Objects of arbitrary content types upload, persist metadata, download byte-for-byte intact, and match SHA-256."

ensure_cluster_ready

TXT_FILE="${ROOT_DIR}/demo/files/sample.txt"
PNG_FILE="${ROOT_DIR}/demo/files/sample.png"

step "1. Uploading sample.txt and sample.png"
TXT_JSON="$(cli upload "$TXT_FILE" --meta purpose=demo1 --meta kind=text --json)"
TXT_ID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$TXT_JSON")"
register_temp_object "$TXT_ID"
run_cmd cli info "$TXT_ID"

PNG_JSON="$(cli upload "$PNG_FILE" --meta purpose=demo1 --meta kind=image --json)"
PNG_ID="$("$PY" -c "import sys,json; print(json.loads(sys.argv[1])['object_id'])" "$PNG_JSON")"
register_temp_object "$PNG_ID"
run_cmd cli info "$PNG_ID"

pause_step

step "2. Downloading both objects and verifying SHA-256 + byte-for-byte cmp"
DL_TXT="${DEMO_TMP_DIR}/dl_sample.txt"
DL_PNG="${DEMO_TMP_DIR}/dl_sample.png"

run_cmd cli download "$TXT_ID" -o "$DL_TXT"
run_cmd cli download "$PNG_ID" -o "$DL_PNG"

ORIG_TXT_SHA="$(sha256_file "$TXT_FILE")"
DL_TXT_SHA="$(sha256_file "$DL_TXT")"
ORIG_PNG_SHA="$(sha256_file "$PNG_FILE")"
DL_PNG_SHA="$(sha256_file "$DL_PNG")"

echo "sample.txt SHA-256: orig=${ORIG_TXT_SHA} dl=${DL_TXT_SHA}"
echo "sample.png SHA-256: orig=${ORIG_PNG_SHA} dl=${DL_PNG_SHA}"

cmp -s "$TXT_FILE" "$DL_TXT" || fail_msg "sample.txt byte comparison failed"
cmp -s "$PNG_FILE" "$DL_PNG" || fail_msg "sample.png byte comparison failed"
[ "$ORIG_TXT_SHA" = "$DL_TXT_SHA" ] || fail_msg "sample.txt SHA-256 mismatch"
[ "$ORIG_PNG_SHA" = "$DL_PNG_SHA" ] || fail_msg "sample.png SHA-256 mismatch"

echo "MATCH: sample.txt and sample.png verified identical"
pass_msg "Demo 1 completed successfully."
