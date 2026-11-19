# Running benchmarks

## Local stack (docker compose)

```bash
make venv build up            # Python venv, JVM artefacts, Kafka/Redis/ClickHouse/gateway/mock model/Flink
.venv/bin/realy scenarios     # list scenario packs
.venv/bin/realy budget smoke  # Monte Carlo feasibility before spending a run
.venv/bin/realy run smoke     # writes runs/<run_id>/report.{json,md,html}; exit code = verdict
.venv/bin/realy verify runs/<run_id>
```
`make up-obs` adds Prometheus (:9090) and Grafana (:3000, dashboard "realy — pipeline").
The operator console: `make ui console` → http://localhost:8090.

## What a run does

1. builds/loads the world, creates topics, seeds profile blobs in Redis;
2. pushes the scenario's `ScoringConfig` / `ModelServerConfig` (admin HTTP + `bench.control`),
   enables the gateway's Kafka ingress for async-gateway topologies, and puts the right Flink job
   in place (stop-with-savepoint keeps state; `flink_state: fresh` starts clean);
3. optional warm-start backfill, then waits for the feature job to catch up;
4. starts collector(s), then emitter shards on a common schedule origin (open loop);
5. streams `bench.metrics` into `metrics.jsonl`, enforces guardrails (abort on backlog / schedule lag);
6. drains, writes Parquet + the report; exit code 0 = PASS (or matches `expect.verdict`).

## Reading the report

* **Latency by step** — every step from on-time publishing (sent − intended) through produce ack,
  ingress, each Flink stage, feature lookup, model (client / server queue / server compute / batch
  wait), rules, policy, audit ack, egress, to end-to-end. Each row is an independent distribution.
* **Latency vs throughput** — per TPS level of a step/ramp/spike profile (first 20% of each level
  skipped as transition). The **knee** is the first level whose e2e p99 exceeds the budget or whose
  decided rate falls behind the emitted rate.
* **Throughput funnel** — emitted / acked / decided / collected per second and the backlog between
  them: where events pile up.
* **Tail attribution** — for the slowest 1%, mean time per stage and its share.
* **Sustainability** — p99 per window with a drift slope, and whether the backlog grows.
* **Detection** — recall/precision/FPR overall, per typology (incl. fraud approved before the first
  decline in a campaign), and per rule (which rules flag legitimate traffic).
* **Verdict** — PASS only if: no material loss, e2e p99 ≤ budget, hot-path p99 ≤ budget, compliance ≥
  `min_compliance`, sustainable, not aborted.

## Scenario reference (`scenarios/*.yaml`)

```yaml
name: my-scenario
seed: 42                                     # determinism
world: { customers: 2000000, merchants: 100000, payees: 200000 }
traffic:
  profile: { kind: step, start_tps: 1000, step_tps: 1000, step_duration_s: 30 }  # constant|ramp|step|spike|sine
  arrivals: uniform                          # or poisson
  duration_s: 300
  warmup_s: 10                               # excluded from stats
  time_scale: auto                           # sim-s per wall-s (auto = realistic per-customer behaviour)
  generators: 2                              # emitter shards (processes)
  backfill_days: 1                           # warm-start history (features only)
topology: gateway-kafka                      # gateway-kafka | gateway-grpc | flink-inline | echo-kafka
sync: { mode: unary, channels: 4 }           # gateway-grpc only (unary | stream)
rules: baseline                              # rules/<name>.yaml (CEL)
scoring:                                     # realy.bench.v1.ScoringConfig (proto3 JSON)
  budget_ms: 100
  feature_deadline_ms: 30
  model_deadline_ms: 80
  on_deadline: FALLBACK_RULES                # | TIMEOUT | PROVISIONAL_THEN_ASYNC
  feature_lookup_delay: { lognormal: { median_ms: 2, p99_ms: 8 } }   # fixed|uniform|normal|lognormal|bimodal|empirical
  model_extra_delay: { fixed: { ms: 0 } }
  microbatch_window_us: 0
  self_inclusion: true
model_server:                                # realy.bench.v1.ModelServerConfig
  service_time: { uniform: { min_ms: 30, max_ms: 100 } }
  workers: 64                                # 0 = unlimited (a pure delay mock)
  max_batch: 1
  max_queue_delay_us: 0
  max_queue: 0
fraud:
  background_per_1k: 0.5
  campaigns: [ { kind: atm_cashout, at_s: 40, params: { cards: 2000 } } ]
budget: { e2e_p99_ms: 150, hot_path_p99_ms: 100, min_compliance: 0.99 }
guardrails: { max_consumer_lag: 500000, max_schedule_lag_ms: 2000 }
flink_state: warm                            # fresh => exact `realy verify`
exactly_once: false
expect: { verdict: FAIL }                    # for scenarios that must fail (CI)
```
Any key can be overridden: `realy run smoke --set traffic.profile.tps=2000 --set scoring.budget_ms=80`.

## Live control during sustained runs

```bash
realy control emitter --run-id <id> --tps 2000                         # live rate change
realy control model --model-server '{"service_time":{"lognormal":{"median_ms":20,"p99_ms":60}}}'
realy control '*' --scoring '{"feature_deadline_ms": 15}'
realy control '*' --run-id <id> --abort
```
All services subscribe to the compacted `bench.control` topic (latest config survives restarts) and
apply updates with *presence* semantics (every field present overrides, absent fields are kept).

## Training proxy models

```bash
realy train --run runs/<a> --run runs/<b> --hidden 64,32 --trees 300   # served vectors + labels
MODEL_WEIGHTS=/models/proxy/fraud_mlp.weights.json make up             # mock server uses the trained MLP
```
Outputs `models/proxy/fraud_mlp.weights.json` (mock server), `models/triton/fraud_mlp` (ONNX,
TensorRT FP16 on GPU) and `models/triton/fraud_gbdt` (XGBoost, FIL). The verifier selects the weights
file matching the `model_version` recorded in the decisions.
