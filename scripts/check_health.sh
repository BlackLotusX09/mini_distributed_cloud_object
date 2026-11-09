#!/usr/bin/env bash
# Hit /health on the controller (8000) and workers (8001-8004). Exit 1 if any fails.
fail=0
for port in 8000 8001 8002 8003 8004; do
  if out=$(curl -sf --max-time 3 "http://localhost:${port}/health"); then
    echo "OK   :${port} ${out}"
  else
    echo "FAIL :${port}"; fail=1
  fi
done
exit $fail

