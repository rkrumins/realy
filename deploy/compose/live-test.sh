#!/usr/bin/env bash
# Live acceptance test of a running Docker stack (`make up` first): `make live-test`.
#
#   1. the in-container suite (realy live-test), driven through the console API:
#      launch + live steering + report accuracy vs ClickHouse, abort in MEASURE and in BACKFILL,
#      guardrail abort, a killed collector, select/clear/abort-all, stop all, cross-process
#      lease + abort (console <-> CLI), SIGTERM, and a healthy pipeline afterwards;
#   2. faults only the host can inject, each against a live run:
#      gateway, Kafka broker, Flink TaskManager and Redis restarts (zero loss expected), and the
#      console stopped gracefully (run aborted with a partial report) or SIGKILLed (run interrupted,
#      recovered on restart);
#   3. a final clean run.
#
# Env: LIVE_CASES="abort_measure guardrail" runs a subset of step 1; LIVE_FAULTS=0 skips step 2.
set -uo pipefail
cd "$(dirname "$0")/../.."
DC=(docker compose -f deploy/compose/docker-compose.yml)
EX=("${DC[@]}" exec -T console)
rc=0
results=()

wait_console() {
  for _ in $(seq 1 90); do
    if "${EX[@]}" python -c "import httpx,sys; sys.exit(0 if httpx.get('http://localhost:8090/api/health',timeout=3).json().get('ok') else 1)" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "console did not come back"; return 1
}

echo "=== 1. in-container suite"
cases=()
for c in ${LIVE_CASES:-}; do cases+=(--case "$c"); done
if "${EX[@]}" realy live-test "${cases[@]}"; then results+=("PASS suite"); else results+=("FAIL suite"); rc=1; fi

fault() {  # $1 label, $2 expectation, $3.. the fault command
  local label=$1 expect=$2; shift 2
  echo; echo "=== fault: $label"
  local ids
  if ! ids=$("${EX[@]}" realy live-test fault-start --duration 90 | tail -1); then
    results+=("FAIL $label (run did not start)"); rc=1; return
  fi
  echo "run: $ids; injecting: $*"
  "$@"
  wait_console || { results+=("FAIL $label (console down)"); rc=1; return; }
  # shellcheck disable=SC2086
  if "${EX[@]}" realy live-test fault-check $ids --expect "$expect"; then
    results+=("PASS $label")
  else
    results+=("FAIL $label"); rc=1
  fi
}

if [ "${LIVE_FAULTS:-1}" != "0" ]; then
  echo; echo "=== 2. host-injected faults"
  fault "gateway restart"            zero-loss   "${DC[@]}" restart gateway
  fault "flink taskmanager restart"  zero-loss   "${DC[@]}" restart flink-taskmanager
  fault "redis restart"              zero-loss   "${DC[@]}" restart redis
  fault "kafka broker restart"       zero-loss   "${DC[@]}" restart kafka
  fault "console stop (graceful)"    aborted     "${DC[@]}" restart console
  fault "console crash (SIGKILL)"    interrupted sh -c "${DC[*]} kill -s KILL console && ${DC[*]} up -d console"
fi

echo; echo "=== 3. clean run afterwards"
if "${EX[@]}" realy live-test --case final_health; then results+=("PASS final health"); else results+=("FAIL final health"); rc=1; fi

echo; echo "=== live acceptance summary"
printf '  %s\n' "${results[@]}"
exit $rc
