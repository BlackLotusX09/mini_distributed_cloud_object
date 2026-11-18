#!/usr/bin/env bash
# Run all 6 automated demos sequentially, check cluster invariants, and print a summary table.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib.sh"

banner "DISTRIBUTED OBJECT STORE — FULL DEMO SUITE (Demos 1..6)"

# Start a shared cluster once if neither Docker nor a running controller is up
if ! curl -sf --max-time 2 "${BASE_URL}/health" >/dev/null 2>&1; then
  detect_demo_mode
  if [ "$DEMO_MODE" = "docker" ]; then
    fail_msg "Controller not reachable at ${BASE_URL}; start Docker stack with: docker compose up -d --build"
  fi
  step "Starting shared local cluster for all 6 demos (Docker daemon unavailable)"
  start_local_cluster
  export DFS_SHARED_LOCAL=1 LOCAL_CLUSTER_ROOT LOCAL_DB_PATH WORKER_DATA_ROOT="$LOCAL_CLUSTER_ROOT"
else
  detect_demo_mode
fi

DEMOS=(
  "demo_1_storage.sh"
  "demo_2_chunking.sh"
  "demo_3_replication.sh"
  "demo_4_metadata.sh"
  "demo_5_failure.sh"
  "demo_6_integrity.sh"
)

declare -a NAMES=()
declare -a RESULTS=()
declare -a DURATIONS=()
TOTAL_FAIL=0
EXTRA_FLAGS=()
if [ "$NON_INTERACTIVE" = "1" ]; then
  EXTRA_FLAGS+=("--non-interactive")
fi

for d in "${DEMOS[@]}"; do
  t0="$(date +%s)"
  banner "RUNNING ${d}"
  if [ "$d" = "demo_6_integrity.sh" ]; then
    set +e
    bash "${ROOT_DIR}/scripts/${d}" "${EXTRA_FLAGS[@]}" --include-unrecoverable
    rc=$?
    set -e
  else
    set +e
    bash "${ROOT_DIR}/scripts/${d}" "${EXTRA_FLAGS[@]}"
    rc=$?
    set -e
  fi
  t1="$(date +%s)"
  NAMES+=("$d")
  DURATIONS+=("$((t1 - t0))s")
  if [ "$rc" = "0" ]; then
    RESULTS+=("PASS")
  else
    RESULTS+=("FAIL (exit ${rc})")
    TOTAL_FAIL=$((TOTAL_FAIL + 1))
  fi
done

banner "RUNNING FINAL INVARIANT CHECK (scripts/check_invariants.py)"
set +e
run_invariant_check
INV_RC=$?
set -e

echo ""
printf "%-26s | %-15s | %s\n" "Demo Script" "Result" "Elapsed"
printf "%-26s-+-%-15s-+-%s\n" "--------------------------" "---------------" "-------"
for i in "${!NAMES[@]}"; do
  printf "%-26s | %-15s | %s\n" "${NAMES[$i]}" "${RESULTS[$i]}" "${DURATIONS[$i]}"
done
printf "%-26s-+-%-15s-+-%s\n" "--------------------------" "---------------" "-------"
if [ "$INV_RC" = "0" ]; then
  printf "%-26s | %-15s | %s\n" "check_invariants.py" "PASS" "-"
else
  printf "%-26s | %-15s | %s\n" "check_invariants.py" "FAIL (exit ${INV_RC})" "-"
  TOTAL_FAIL=$((TOTAL_FAIL + 1))
fi

echo ""
if [ "$TOTAL_FAIL" = "0" ]; then
  pass_msg "All 6 demos and cluster invariant checks passed!"
  exit 0
else
  fail_msg "${TOTAL_FAIL} check(s) failed."
fi
