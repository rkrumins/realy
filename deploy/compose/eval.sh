#!/usr/bin/env bash
# Honest detection evaluation inside the Docker stack (`make eval`):
#   1. fraud-train (seed 7): its own world; served features + observed labels are the training data
#   2. train two proxies on it: observed labels (what a bank knows 30 days later) and ground truth
#   3. hot-deploy each in turn and score fraud-eval (seed 1337, a world no training run used),
#      verify the run (features, rules, scores, ClickHouse audit)
#   4. replay the previous rule set (baseline-v1) over the evaluation run: the ATO rule change measured
#      on unseen data
#   5. tail-stress with and without model-call hedging
# Prints the run ids; every number is in /runs/<run_id>/report.{json,md,html}.
set -uo pipefail
cd "$(dirname "$0")/../.."
DC=(docker compose -f deploy/compose/docker-compose.yml)
EX=("${DC[@]}" exec -T console)
TS=$(date -u +%Y%m%d-%H%M%S)
out=()

run() {  # run_id scenario [--set ...]
  local id=$1 sc=$2; shift 2
  echo "=== realy run $sc ($id)"
  "${EX[@]}" realy run "$sc" --run-id "$id" "$@" | grep -E "^=== |^emitted|^e2e|^reasons|^detection|^evaluation|^model alone|^report" || true
  out+=("$id")
}

# TRAIN_RUN=<run id> reuses a finished fraud-train run (resume after a failure)
TRAIN="${TRAIN_RUN:-fraud-train-$TS}"
[ -n "${TRAIN_RUN:-}" ] || run "$TRAIN" fraud-train

echo "=== train (observed labels)"
OBS=$("${EX[@]}" realy train --run "/runs/$TRAIN" --labels observed | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["weights_path"]); print("labels", json.dumps(d["label_stats"]), "metrics", json.dumps(d["metrics"]), file=sys.stderr)')
echo "=== train (ground truth)"
GT=$("${EX[@]}" realy train --run "/runs/$TRAIN" --labels ground-truth | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["weights_path"]); print("labels", json.dumps(d["label_stats"]), "metrics", json.dumps(d["metrics"]), file=sys.stderr)')
[ -n "$OBS" ] && [ -n "$GT" ] || { echo "training failed"; exit 1; }

"${EX[@]}" realy deploy-model "$OBS"
run "fraud-eval-obs-$TS" fraud-eval
"${EX[@]}" realy verify "/runs/fraud-eval-obs-$TS" | tail -12

"${EX[@]}" realy deploy-model "$GT"
run "fraud-eval-gt-$TS" fraud-eval

echo "=== baseline-v1 (ATO rule as DECLINE) replayed over the evaluation run"
"${EX[@]}" realy rules-whatif "/runs/fraud-eval-obs-$TS" --rules /opt/realy/rules/baseline-v1.yaml

"${EX[@]}" realy deploy-model "$OBS"
run "lost-stolen-$TS" lost-stolen-contactless
run "tail-stress-nohedge-$TS" tail-stress
run "tail-stress-hedge-$TS" tail-stress --set scoring.hedge_after_ms=30

echo; echo "runs: ${out[*]}"
