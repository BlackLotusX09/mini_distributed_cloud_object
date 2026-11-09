#!/usr/bin/env bash
# Run controller + 4 workers as local processes (no Docker) and check health/registration.
cd "$(dirname "$0")/.." || exit 1
T=/tmp/ds_t; rm -rf $T; mkdir -p $T
pids=()
(cd controller && DB_PATH=$T/m.db PORT=8000 exec python3 app.py >$T/c.log 2>&1) & pids+=($!)
for i in 1 2 3 4; do
  (cd worker && WORKER_ID=worker$i PORT=800$i DATA_DIR=$T/w$i CONTROLLER_URL=http://localhost:8000 \
    HEARTBEAT_SEC=1 WORKER_ADDRESS=http://localhost:800$i exec python3 app.py >$T/w$i.log 2>&1) & pids+=($!)
  [ "$i" = 2 ] && w2=$!
done
trap 'kill "${pids[@]}" 2>/dev/null' EXIT
sleep 3
bash scripts/check_health.sh; rc=$?
echo "--- registered workers:"; curl -s localhost:8000/internal/workers; echo
kill $w2 2>/dev/null; sleep 1
echo "--- after killing worker2 (expect :8002 FAIL):"; bash scripts/check_health.sh
exit $rc
