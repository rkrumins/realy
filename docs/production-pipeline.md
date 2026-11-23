# From benchmark to production: the payment-scoring pipeline

What PulseBench benchmarks is the pipeline you run: the same gateway, Flink job, contracts, rules and
model integration, built from the same images. This page separates the pipeline from the instrument,
explains how to run the pipeline on its own for real payments, lists the guarantees it gives and how each
is met, and describes the loop that keeps the benchmark pointed at it afterwards.

## Pipeline and instrument

| The pipeline (deploy it) | The instrument (keep it for staging and CI) |
|---|---|
| contracts: messages, event bindings, transactions, features, rules (`contracts/`, `rules/`) | the generator and emitter, the collector |
| the scoring gateway (`java/scoring-gateway`): sync gRPC / HTTP and async Kafka ingress | the runner, the verifier and its Python twins |
| the Flink job (`java/flink-job`): assembly, features, inline scoring | the console, reports, Explorer |
| the scoring core (`java/scoring-core`): features, CEL rules, policy, deadlines, model client | `bench.control` (live config pushes), `bench.metrics` (1 s snapshots), the `x-bench-trace` header |
| your Kafka, Redis, model server (Triton / KServe), ClickHouse or warehouse | |

The pipeline does not need the instrument. Events without a trace header are scored like any other. The
control and metrics topics are optional, and the production profile turns them off.

## The production profile

`REALY_PROFILE=production` on the gateway and the Flink job:

| Behaviour | Why |
|---|---|
| the control topic is not subscribed (`CONTROL_ENABLED=false` unless set) | on a cluster shared with a benchmark, a run's commands (echo, injected delays, ingress switches) must never reach production |
| admin writes (`PUT /admin/config`) need `ADMIN_TOKEN(_FILE)`, and are refused without one | the admin port can change rules and thresholds |
| benchmark-only settings are refused at start and on reload: echo, injected latency, the mock store | they would approve without scoring, slow decisions on purpose, or score without features |
| the model's input width must come from its metadata or `MODEL_INPUT_DIM` | a guessed width makes every call fail or mis-score |

## Guarantees, and how each is met

| Guarantee | How |
|---|---|
| **Every event gets a durable decision, or a dead letter.** | A scoring error, thrown or asynchronous, becomes a fail-safe decision: `ON_PIPELINE_ERROR`, default REVIEW, reason `PIPELINE_ERROR`, flagged degraded. A record that cannot be read, or lacks an event, customer or time id, goes to the dead-letter topic `payments.events.dlq` with its bytes, headers and the error. This holds in the gateway and in the Flink job, where a poison record would otherwise restart-loop the job. |
| **A decision counts only once it is durable.** | The async rail commits a source offset only below the oldest decision not yet durable; the sync rail replies after the audit write. Durable means one of three things: (1) acknowledged by the broker (`acks=all`, idempotent); (2) when the broker cannot take it, appended to the audit spool and fsynced, framed with length and CRC; (3) if the spool fails too, retried until one of them takes it, while the offset is held. |
| **The spool reaches the broker, once.** | A background replayer ships the spool while the gateway runs. It rotates the file first, persists its position after every ack, and cuts off a record torn by a crash. The spool belongs on a persistent volume; the production overlay gives each gateway pod its own. |
| **At-least-once, with duplicates removable.** | After a crash, events whose decisions were not durable are decided again. Consumers keep one decision per `(event_id, revision)`: ClickHouse's `ReplacingMergeTree` does it for the audit. Revision 1 is the final decision after a provisional answer (`PROVISIONAL_THEN_ASYNC`). The Flink job can run exactly-once (`exactly_once`, transactional sink, read-committed consumers), at the cost of the checkpoint interval in visibility. |
| **A slow dependency does not become a missing decision.** | Feature and model deadlines (`feature_deadline_ms`, `model_deadline_ms`) give a rules-only decision, flagged degraded. `on_deadline: PROVISIONAL_THEN_ASYNC` answers now and publishes the final decision later; `TIMEOUT` fails the request. Rules-only is fail-open by design for card authorisation. Choose it deliberately, and alert on the degraded rate. |
| **A wrong model answer is an error, not an approval.** | A missing, NaN or out-of-range score is a model error and goes through the deadline policy. |
| **Shutdown loses nothing.** | On SIGTERM the gateway stops polling, waits for the decisions in flight to become durable (`DRAIN_TIMEOUT_MS`), commits, then closes the audit producer. `terminationGracePeriodSeconds` must exceed that. |

## Configuration

| What | Where |
|---|---|
| endpoints and switches | environment (`deploy/k8s/overlays/production/config/pipeline.env`): every variable is in [reference/environment.md](reference/environment.md) |
| scoring config (budgets, deadlines, thresholds, audit policy) | `SCORING_CONFIG_FILE` (proto3 JSON), hot-reloaded when the file changes (a ConfigMap update) |
| rules | `RULES_FILE` (a rule-set YAML, [reference/rules.md](reference/rules.md)), hot-reloaded; a file that does not parse is logged and the active rules kept |
| feature schema, bindings, transactions | the image's `contracts/`, or `FEATURE_SCHEMA` / `REALY_CONTRACTS` for mounted ones |
| secrets | files: `KAFKA_SASL_PASSWORD_FILE`, `MODEL_AUTH_TOKEN_FILE`, `ADMIN_TOKEN_FILE`, `CLICKHOUSE_PASSWORD_FILE`; `REDIS_URI` from a Secret |
| the ingress topic | `INGRESS_TOPIC`: the assembled topic when Flink assembles multi-message transactions, else the events topic; `INGRESS_KAFKA=true`; `KAFKA_OFFSET_RESET=earliest` for a first deploy that must not skip a backlog |

## Health, metrics, alerts

- `GET /ready` is the readiness probe. The gateway is ready when every ingress loop polled in the last 10 s, the audit broker is acknowledging, the store's connections are open and the model channel is not failing. It returns the checks as JSON.
- `GET /health` is the liveness probe: the process answers.
- `GET /admin/info` reports what the gateway is connected to (no credentials).
- `GET /metrics` serves Prometheus metrics. Alert on:

| Metric | Means |
|---|---|
| `degraded` / `decisions` | rules-only decisions: the model or store missed its deadline |
| `pipeline_errors` | fail-safe decisions after a scoring error |
| `poison_records` | dead-lettered records |
| `audit_failures`, the spool size in `/ready` | the broker is not taking decisions |
| `ingress_restarts` | a consumer loop failed and was restarted |
| `skipped_mirrored`, `skipped_backfill`, `skipped_parts` | records deliberately not scored (headers) |
| `hot_path` p99 | the decision latency, against the budget |

## Deploying

`deploy/k8s/overlays/production`: the pipeline alone, on your infrastructure.

| Part | What it is |
|---|---|
| gateway | a StatefulSet (a persistent spool per pod), readiness on `/ready`, zone spread, a PodDisruptionBudget, an HPA on CPU (at most partitions / consumers per pod) |
| Flink | the features job through the Flink Kubernetes Operator, with checkpoints, savepoints and HA on durable storage, savepoint upgrades |
| config | the pipeline-env and scoring-config ConfigMaps, the pipeline-secrets Secret (shape in `secrets.example.yaml`) |

Topics: create those in `contracts/kafka/topics.yaml` with replication 3 and `min.insync.replicas` 2.
Size them with `pulsebench size` from a measured run.

**Upgrades:**
- **Contracts are append-only:** new fields and features at the end, enforced by the compatibility test.
- **Flink restores keyed state** across releases with stable operator ids. A change that would drop state forces fresh state with the reason, rather than silently.
- **Gateway rollouts** are rolling restarts; drain keeps them lossless.

## The loop: keep benchmarking the platform

1. **Design in contracts:** messages, transactions, features, rules.
2. **Benchmark on the bundled stack:** verify parity, find the knee, read the advice.
3. **Point the benchmark at staging,** which runs the production overlay on your infrastructure
   ([real-infrastructure.md](real-infrastructure.md)). The preflight confirms what it runs against, and the
   report says what is real and what is simulated.
4. **Harden from the advice and the measured tail;** compare runs before and after each change.
5. **Gate in CI:** the regression gate on a staging run, so a slower or less exact release does not ship.
6. **Deploy the same images and contracts to production;** keep running the benchmark against staging for
   every change.

## Readiness review

A review of the pipeline running without the benchmark found the issues below. This is their status:

| Issue | Status |
|---|---|
| unauthenticated admin writes | fixed: token, `ADMIN_WRITE=false`, refused without a token in production |
| the control topic could not be switched off | fixed: `CONTROL_ENABLED`, off in production |
| a poison record restart-looped the Flink job | fixed: dead letter, job continues |
| a synchronous scoring exception killed the gateway's ingress loop | fixed: every error becomes a decision; loops are supervised and restarted |
| an asynchronous scoring error left an event without a decision | fixed: fail-safe decision |
| in-flight permits leaked on a rebalance or topic switch | fixed: released in `finally`, null-safe |
| the audit spool was not durable, and a failed spool counted as durable | fixed: fsync, framing, offsets held until durable |
| spool replay re-sent records, wedged on a torn tail, ran only at start | fixed: background replay with persisted position, tail truncation |
| `/health` was not a readiness check | fixed: `/ready` |
| shutdown did not drain | fixed: drain, then commit, then close |
| the producer could block 60 s | fixed: `KAFKA_MAX_BLOCK_MS` 5 s; mirror failures counted |
| a NaN or short model output approved | fixed: a model error |
| garbage records scored with id 0 | fixed: validated, dead-lettered |
| records skipped by header without a count | fixed: counters; the default event type is scored even when labelled |
| scoring config and rules only from env / control | fixed: files, hot-reloaded |
| the SASL password only as an env value; internal errors returned to gRPC callers | fixed: `_FILE`; generic status with a reference id |
| decisions carry the full event and feature vector | by design (audit, training); lower `feature_sample_permille` and review the decisions topic's access for PII |
| `TransactionEvent` is the scored message type | the unified payment event: other messages enter as parts of transactions; independent event types are the next phase (S4) |
