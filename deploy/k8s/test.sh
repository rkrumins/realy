#!/usr/bin/env bash
# In-cluster acceptance test for realy on Kubernetes (make k8s-test), after deploy/k8s/local-up.sh.
# Everything runs through the console pod (the bench driver lives there), so it tests the real
# in-cluster Kafka + Flink, not a host shortcut.
#
#   1. platform    every pod Ready, kafka-topics + clickhouse-init Jobs complete, all brokers Ready,
#                  topics at the expected replication factor, console /api/env: every component up
#   2. smoke       pulsebench run smoke: zero loss + latency budget PASS (async gateway, Flink features)
#   3. flink       pulsebench run flink-inline-smoke: zero loss + latency budget; pulsebench verify: features exact,
#                  rules/score parity, ClickHouse audit complete
#   4. failover    (3 brokers) smoke for 90 s, one broker pod deleted mid-run -> zero missing decisions;
#                  the broker rejoins and every partition is back to a full ISR
# Reports are copied to ./runs-k8s/. Exit code 0 only if every check passed.
#
# Knobs: REALY_K8S / REALY_CLUSTER (lib.sh), REALY_TEST_SKIP="flink failover" (skip checks),
#        REALY_FAILOVER_AT_S=40 (when to delete the broker, seconds after the run starts),
#        REALY_FAILOVER_BROKER=realy-kafka-1.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
# shellcheck source=deploy/k8s/lib.sh
. "$ROOT/deploy/k8s/lib.sh"
resolve_kubectl
OUT="$ROOT/runs-k8s"
mkdir -p "$OUT"
SKIP=" ${REALY_TEST_SKIP:-} "
RESULTS=()
RUN_IDS=()

record() { RESULTS+=("$1|$2|$3"); printf '  %-4s %-34s %s\n' "$2" "$1" "$3"; }
skipped() { case "$SKIP" in *" $1 "*) return 0 ;; esac; return 1; }
cexec() { k -n "$NS" exec deploy/console -- "$@"; }
# realy prints "report: /runs/<id>/report.html" last; remember the id for the report copy
run_id_of() { sed -n 's#^report: /runs/\([^/]*\)/report.html#\1#p' | tail -1; }

say "realy acceptance test on Kubernetes ($PROVIDER, namespace $NS)"

# ------------------------------------------------------------------------------------------ 1. platform
if k -n "$NS" wait --for=condition=Ready pod -l 'app in (kafka,redis,clickhouse,gateway,mock-model,flink,console)' \
     --timeout=300s >/dev/null 2>&1; then
  record "pods Ready" PASS "$(k -n "$NS" get pods --no-headers 2>/dev/null | grep -c Running) running"
else
  record "pods Ready" FAIL "$(k -n "$NS" get pods --no-headers | grep -v -E 'Running|Completed' | awk '{print $1":"$3}' | tr '\n' ' ')"
fi
if k -n "$NS" wait --for=condition=complete job/kafka-topics job/clickhouse-init --timeout=120s >/dev/null 2>&1; then
  record "init Jobs complete" PASS "kafka-topics, clickhouse-init"
else
  record "init Jobs complete" FAIL "see: kubectl -n $NS logs job/kafka-topics / job/clickhouse-init"
fi
want=$(k -n "$NS" get statefulset realy-kafka -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)
ready=$(k -n "$NS" get statefulset realy-kafka -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
[ "${ready:-0}" = "$want" ] && record "Kafka brokers" PASS "$ready/$want ready" || record "Kafka brokers" FAIL "${ready:-0}/$want ready"

describe() { k -n "$NS" exec realy-kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$1" 2>/dev/null; }
rf=$(describe payments.events.raw | sed -n 's/.*ReplicationFactor: \([0-9]*\).*/\1/p' | head -1)
if [ "${rf:-0}" = "$want" ]; then
  isr_cfg=$(describe payments.events.raw | sed -n 's/.*min.insync.replicas=\([0-9]*\).*/\1/p' | head -1)
  record "topic replication" PASS "payments.events.raw RF ${rf}${isr_cfg:+, min ISR $isr_cfg}"
else
  record "topic replication" FAIL "payments.events.raw RF ${rf:-?} (expected $want)"
fi

envcheck=$(cexec python - <<'EOF' 2>&1
import json, os, urllib.request
req = urllib.request.Request("http://127.0.0.1:8090/api/env")
if os.environ.get("REALY_CONSOLE_TOKEN"):
    req.add_header("Authorization", "Bearer " + os.environ["REALY_CONSOLE_TOKEN"])
d = json.load(urllib.request.urlopen(req, timeout=30))
bad = [f"{c['id']}={c['status']}" for c in d["components"] if c["status"] != "up"]
print(("down: " + " ".join(bad)) if bad else f"{len(d['components'])} components up")
raise SystemExit(1 if bad else 0)
EOF
) && record "console /api/env" PASS "$envcheck" || record "console /api/env" FAIL "$envcheck"

# ------------------------------------------------------------------------------------------ 2./3. scenarios
# Each scenario yields two rows: correctness (every event decided, nothing lost) and the latency budget
# verdict. Correctness must hold on any machine; the latency budget needs CPU headroom - 3 brokers
# replicating plus the whole pipeline on one node want >= 6 vCPUs (see docs/deployment-kubernetes.md).
summary_of() {  # $1 run id -> "missing decided p99 verdict"
  cexec python -c "
import json; s = json.load(open('/runs/$1/report.json'))['summary']
print(s['missing'], s['decided'], round(s['e2e_p99_ms'] or -1), s['verdict'], s.get('within_budget') or 0)" 2>/dev/null
}
LAT_HINT="node CPU-starved? check 'kubectl top pods -n $NS' / give the node more vCPUs or use REALY_KAFKA_BROKERS=1"
run_scenario() {  # $1 label, $2.. pulsebench run args
  local label=$1; shift
  local log rc id missing decided p99 within
  log=$(cexec pulsebench run "$@" 2>&1); rc=$?
  id=$(printf '%s\n' "$log" | run_id_of)
  if [ -z "$id" ]; then
    record "$label" FAIL "run did not complete: $(printf '%s\n' "$log" | grep -iE 'error|exception' | tail -1)"
    LAST_ID=""; return
  fi
  RUN_IDS+=("$id"); LAST_ID=$id
  read -r missing decided p99 _ within <<<"$(summary_of "$id")"
  if [ "${missing:-1}" = 0 ] && [ "${decided:-0}" -gt 0 ]; then
    record "$label: zero loss" PASS "$decided decided, 0 missing"
  else
    record "$label: zero loss" FAIL "${missing:-?} missing of $((${decided:-0} + ${missing:-0}))"
  fi
  if [ $rc = 0 ]; then
    record "$label: latency budget" PASS "$(printf '%s\n' "$log" | grep -E '^e2e p50' | tail -1)"
  else
    record "$label: latency budget" FAIL "e2e p99 ${p99} ms, within budget $(awk "BEGIN{printf \"%.1f%%\", ${within:-0}*100}") - $LAT_HINT"
  fi
}

say "smoke (async gateway, 500 TPS, 60 s)"
run_scenario smoke smoke

if ! skipped flink; then
  say "flink-inline-smoke (Flink does everything) + independent verification"
  run_scenario flink-inline flink-inline-smoke
  if [ -n "$LAST_ID" ]; then
    vlog=$(cexec pulsebench verify "/runs/$LAST_ID" 2>&1); rc=$?
    [ $rc = 0 ] && record "verify (exact features, audit)" PASS "$(printf '%s\n' "$vlog" | grep -E '^audit' | cut -c1-90)" \
                || record "verify (exact features, audit)" FAIL "$(printf '%s\n' "$vlog" | grep -iE 'FAIL|mismatch|Error' | head -2 | tr '\n' ' ')"
  fi
fi

# ------------------------------------------------------------------------------------------ 4. failover
if [ "${want:-0}" -ge 3 ] && ! skipped failover; then
  broker=${REALY_FAILOVER_BROKER:-realy-kafka-1}
  fid="k8s-failover-$(date +%Y%m%d-%H%M%S)"
  say "broker failover: smoke for 90 s, deleting $broker after ${REALY_FAILOVER_AT_S:-40} s"
  # latency is expected to suffer during leader election, so the latency verdict is informational and the
  # schedule-lag guardrail is relaxed; what must hold is RF 3 / min ISR 2 / acks=all -> nothing lost
  cexec pulsebench run smoke --run-id "$fid" --set traffic.duration_s=90 --set guardrails.max_schedule_lag_ms=10000 \
    > "$OUT/$fid.log" 2>&1 &
  pid=$!
  sleep "${REALY_FAILOVER_AT_S:-40}"
  k -n "$NS" delete pod "$broker" --wait=false >/dev/null && echo "  deleted pod $broker"
  wait "$pid"
  RUN_IDS+=("$fid")
  summary=$(cexec python -c "
import json; s = json.load(open('/runs/$fid/report.json'))['summary']
print(f\"missing={s['missing']} duplicates={s['duplicates']} emitted={s['emitted']} decided={s['decided']} e2e_p99={s['e2e_p99_ms']:.0f}ms verdict={s['verdict']}\")
raise SystemExit(0 if s['missing'] == 0 and s['decided'] > 0 else 1)" 2>&1) \
    && record "failover: zero loss" PASS "$summary" || record "failover: zero loss" FAIL "$summary"
  # at-least-once delivery: a consumer-group coordinator move can replay a few uncommitted events;
  # the collector de-duplicates by event id and ClickHouse collapses them, so duplicates are reported only
  if k -n "$NS" wait --for=condition=Ready "pod/$broker" --timeout=180s >/dev/null 2>&1; then
    full=0
    for _ in $(seq 1 24); do
      under=$(k -n "$NS" exec realy-kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
                --describe --under-replicated-partitions 2>/dev/null | grep -c Partition || true)
      [ "${under:-1}" = 0 ] && { full=1; break; }
      sleep 5
    done
    [ $full = 1 ] && record "failover: broker rejoined" PASS "$broker Ready, no under-replicated partitions" \
                  || record "failover: broker rejoined" FAIL "$broker Ready but $under partitions still under-replicated"
  else
    record "failover: broker rejoined" FAIL "$broker not Ready after 180 s"
  fi
fi

# ------------------------------------------------------------------------------------------ reports
for id in "${RUN_IDS[@]}"; do
  cexec sh -c "cd /runs && tar -cf - \$(ls -d $id/report.* $id/verify.json $id/manifest.json 2>/dev/null)" 2>/dev/null \
    | tar -C "$OUT" -xf - 2>/dev/null || true
done

echo
printf '%-36s %-5s %s\n' "CHECK" "" "DETAIL"
fails=0
for r in "${RESULTS[@]}"; do
  IFS='|' read -r name status detail <<<"$r"
  printf '%-36s %-5s %s\n' "$name" "$status" "$detail"
  [ "$status" = PASS ] || fails=$((fails + 1))
done
echo
if [ $fails = 0 ]; then
  say "all ${#RESULTS[@]} checks passed - reports in $OUT"
else
  printf '\033[1;31m%d of %d checks failed - reports and logs in %s\033[0m\n' "$fails" "${#RESULTS[@]}" "$OUT"
fi
exit $((fails > 0))
