#!/usr/bin/env bash
# Flip one byte in a stored chunk file on a worker container (or local WORKER_DATA_ROOT).
# Usage: bash scripts/corrupt_chunk.sh <worker_container_or_name> [chunk_id]
set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "Usage: $0 <worker_container_or_name> [chunk_id]" >&2
  exit 2
fi

WORKER="$1"
CHUNK_ID="${2:-}"

use_docker=0
if [ -z "${WORKER_DATA_ROOT:-}" ] && command -v docker >/dev/null 2>&1; then
  if docker inspect "$WORKER" >/dev/null 2>&1; then
    use_docker=1
  fi
fi

if [ "$use_docker" = "1" ]; then
  docker exec -e TARGET_CHUNK="$CHUNK_ID" "$WORKER" python3 -c '
import hashlib, os, random, sys

root = "/data/chunks"
cid = os.environ.get("TARGET_CHUNK", "").strip()
matches = []
for dp, _, files in os.walk(root):
    for f in sorted(files):
        if f.endswith(".chunk") and not f.startswith("."):
            full = os.path.join(dp, f)
            if not cid or f == f"{cid}.chunk":
                matches.append(full)

if not matches:
    print(f"ERROR: no matching chunk found under {root} (chunk_id={cid!r})", file=sys.stderr)
    sys.exit(1)

target = matches[0] if cid else random.choice(matches)
with open(target, "rb") as f:
    data = bytearray(f.read())
if not data:
    print(f"ERROR: chunk {target} is empty (0 bytes); cannot flip byte", file=sys.stderr)
    sys.exit(1)

before = hashlib.sha256(data).hexdigest()
pos = len(data) // 2
data[pos] ^= 0xFF
with open(target, "wb") as f:
    f.write(data)
    f.flush()
    os.fsync(f.fileno())
after = hashlib.sha256(data).hexdigest()

print(f"target={target}")
print(f"sha256_before={before}")
print(f"sha256_after={after}")
'
else
  # Local filesystem fallback when WORKER_DATA_ROOT is used without Docker
  DATA_DIR="${WORKER_DATA_ROOT}/${WORKER}"
  if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: worker container '$WORKER' not found in docker and directory '$DATA_DIR' does not exist" >&2
    exit 1
  fi
  TARGET_DIR="$DATA_DIR" TARGET_CHUNK="$CHUNK_ID" python3 -c '
import hashlib, os, random, sys

root = os.path.join(os.environ["TARGET_DIR"], "chunks")
cid = os.environ.get("TARGET_CHUNK", "").strip()
matches = []
for dp, _, files in os.walk(root):
    for f in sorted(files):
        if f.endswith(".chunk") and not f.startswith("."):
            full = os.path.join(dp, f)
            if not cid or f == f"{cid}.chunk":
                matches.append(full)

if not matches:
    print(f"ERROR: no matching chunk found under {root} (chunk_id={cid!r})", file=sys.stderr)
    sys.exit(1)

target = matches[0] if cid else random.choice(matches)
with open(target, "rb") as f:
    data = bytearray(f.read())
if not data:
    print(f"ERROR: chunk {target} is empty (0 bytes); cannot flip byte", file=sys.stderr)
    sys.exit(1)

before = hashlib.sha256(data).hexdigest()
pos = len(data) // 2
data[pos] ^= 0xFF
with open(target, "wb") as f:
    f.write(data)
    f.flush()
    os.fsync(f.fileno())
after = hashlib.sha256(data).hexdigest()

print(f"target={target}")
print(f"sha256_before={before}")
print(f"sha256_after={after}")
'
fi
