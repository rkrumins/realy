# Components

One section per component, in the same shape: what it does, how it works, what goes in and out,
how it is configured and scaled, how it fails, where the code and tests are, and how to extend it.
The one-paragraph tour is in [architecture](architecture.md#2-components-and-their-roles); every
message is in [reference/messages.md](reference/messages.md); every environment variable in
[reference/environment.md](reference/environment.md).

## Synthetic world and generator

- **Does:** builds a deterministic synthetic bank (customers, accounts, cards, devices, merchants and
  terminals, payees, mules) and generates its transactions: everyday behaviour plus fraud campaigns
  from the typology registry, with ground-truth labels and label delays.
- **How:** `world/build.py` builds the world from a seed and spec and saves it as Parquet
  (`data/world-<customers>-<seed>/`, content-hashed). `gen/behaviour.py` draws each customer's payments
  from their habits (merchants, amounts, channels, travel); `gen/campaigns.py` and `gen/steps.py` run the
  typologies (`contracts/usecases/typologies/*.yaml`); `gen/auth.py` draws authorisation signals; all
  draws are seeded, so the same scenario produces the same events.
- **In / out:** a scenario → event batches (numpy columns) with labels, consumed by the emitter.
- **Config:** scenario `world:`, `fraud:` (mix, rates, step-up, auth), typology YAML; `REALY_USECASES_USER`.
- **Scales:** the world file per million customers; generation runs in N shard processes (`generators:`).
- **Fails:** a changed spec or seed builds a new world (never mixes worlds); a typology error fails the
  scenario's validation before anything runs.
- **Code / tests:** `python/realy/world`, `python/realy/gen`, `python/realy/usecases.py`;
  `tests/test_gen*.py`, `tests/test_usecases*.py`.
- **Extend:** add a typology as YAML (no code), a behaviour knob in the world spec, or a pattern in
  `gen/steps.py` ([fraud detection](fraud-detection.md), [synthetic data](synthetic-data.md)).

## Emitter

- **Does:** sends the events to Kafka on an open-loop schedule (each event has an intended send time;
  late sends are measured, never absorbed), plus the history replay and the labels at their simulated
  time.
- **How:** `emit/driver.py` runs generator and emitter processes per shard; librdkafka producer with
  latency settings; headers `x-bench-trace`, `x-run-id`, `x-backfill`. For high rates and the sync rail
  the Java load emitter replays a precompiled dataset.
- **In / out:** generator batches → `payments.events.raw`, `payments.labels`; event log
  `events-*.parquet`, emit log `emit-*.parquet`; `bench.metrics` (emitted, acked, schedule lag).
- **Config:** scenario `traffic:` (profile, rate, duration, warm-up, history, generators); live rate
  changes on `bench.control`.
- **Scales:** shards in parallel processes; the schedule-lag guardrail aborts a run whose instrument
  falls behind.
- **Fails:** produce errors count against the error-rate guardrail; an emitter process that dies ends
  the run with the reason.
- **Code / tests:** `python/realy/emit`, `java/load-emitter`; `tests/test_emit*.py`.

## Kafka topics

- **Does:** the transport and the audit trail: events in, decisions out, labels, control, metrics.
- **How:** topics from `contracts/kafka/topics.yaml` (keys, partitions, retention, headers); every
  client pins latency settings (`linger.ms=1`, `fetch.min.bytes=1`).
- **Config:** `KAFKA_BOOTSTRAP` and `KAFKA_SECURITY_*` (TLS / SASL) for every client.
- **Scales:** partitions (compose 12, production 48); the customer key keeps one customer's events
  ordered on one partition.
- **Fails:** producers are idempotent with `acks=all`; the gateway and Flink commit offsets only after
  processing (at-least-once); ClickHouse keeps its consumer offsets.

## Flink feature job

- **Does:** computes the stream features (windows per customer, card, device, IP, merchant, terminal,
  payee), writes each entity's blob to the online store, and in the inline topology scores the payment.
- **How:** seven keyed stages over RocksDB state, driven by the feature DSL; checkpoint-aligned store
  writes; see [the Flink job](dev/flink-job.md) for the graph, state, fault tolerance and how to read
  checkpoints and backpressure.
- **In / out:** `payments.events.raw` → Redis `fs:<entity>:<id>` (and, inline, `payments.decisions`).
- **Config:** job arguments (`--mode`, `--parallelism`, `--max-parallelism`, checkpointing) and the
  TaskManager environment (`STORE_*`, `REDIS_*`); cluster config in compose / Kubernetes.
- **Scales:** parallelism up to 128 key groups; one subtask per key group range; TaskManagers × slots.
- **Fails:** restarts with backoff from the last checkpoint; tolerates slow checkpoints; exits a
  TaskManager on OOM; a lost job is restored from its retained checkpoint.
- **Code / tests:** `java/flink-job`, `java/scoring-core/.../features`; `FeatureJobTest`,
  `StateCodecTest`, `StoreWriterTest`.

## Online feature store (Redis)

- **Does:** serves features to the gateway in one round trip per event.
- **How:** stream blobs `fs:<entity>:<id>` written by Flink (customer and card carry their aggregator
  state, so the gateway re-windows them exactly at the event's time; others carry values), profile
  blobs `pf:<entity>:<id>` seeded from the world. Layout: `contracts/storage/redis.md`.
- **Config:** `REDIS_URI`; Redis `REDIS_MAXMEMORY`, `noeviction`.
- **Scales:** about 200 B per key including Redis's overhead (compact state blobs are ~160 B on
  average); one GET pipeline per event; shard by key for large worlds.
- **Fails:** a full store rejects writes (Flink fails the task, the run's health watch aborts with the
  reason); a restart without persistence loses blobs (the next run reseeds profiles and rebuilds state;
  a run in progress is aborted with the reason).

## Scoring core and gateway

- **Does:** decides each payment within its budget: features → model → rules → policy → decision.
- **How:** `ScoringOrchestrator` looks up the store (one pipelined GET), assembles the 62-feature vector
  in schema order (`FeatureAssembler`, `DerivedFeatures`), calls the model with a deadline, evaluates the
  CEL rules (`RuleSet`) and applies the policy (DECLINE rule > ALLOW > score ≥ decline > CHALLENGE >
  REVIEW). Deadlines fall back to rules-only (`RULES_ONLY_FALLBACK`); hedging can send a provisional
  decision first. Every stage appends a span.
- **In / out:** Kafka ingress or the gRPC `ScoringService` → `payments.decisions`.
- **Config:** `ScoringConfig` (budget, deadlines, thresholds, rules, hedging) on `bench.control` or
  `POST /admin/config`; `KAFKA_CONSUMERS`, `REDIS_*`, `MODEL_TARGET`.
- **Scales:** stateless: replicas × consumers up to the partition count.
- **Fails:** a slow store or model degrades to rules-only within the budget (counted as degraded); the
  audit sink spools to disk if Kafka is down.
- **Code / tests:** `java/scoring-core`, `java/scoring-gateway`; `*Test` under each.
- **Extend:** a rule in `rules/*.yaml`, a feature in the schema, a model behind the KServe contract.

## Model server

- **Does:** scores the feature vector (fraud probability).
- **How:** KServe v2 gRPC; the mock server runs the trained proxy MLP with a configurable service-time
  distribution, workers, batching and queue (`ModelServerConfig`); Triton serves the ONNX export the same
  way. Models bind inputs by feature name, so a model may use a subset of the schema.
- **Config:** `MODEL_WEIGHTS`, `ModelServerConfig`; hot-swap with `pulsebench deploy-model`.
- **Scales:** workers and batching per replica; replicas behind the gateway's client.
- **Code / tests:** `java/mock-model-server`, `python/realy/train`, `models/`.

## ClickHouse

- **Does:** the audit warehouse: every event, decision (with features and spans) and label, for reports,
  training datasets (`v_training_dataset`) and rule performance (`v_rule_performance`).
- **How:** Kafka engine tables and materialised views from the protobuf contracts; DDL generated by
  `python -m realy.codegen` and applied idempotently with `make migrate`.
- **Fails:** consumer offsets survive restarts; the runner waits (bounded) for its backlog before
  measuring.

## Collector

- **Does:** reads the decisions, joins them to the emitted events and measures end-to-end latency
  (intended send → collected), completeness (missing, duplicates) and per-stage timings.
- **Out:** `decisions-*.parquet`, `features-*.parquet`, `bench.metrics`.
- **Code:** `python/realy/collect`.

## Runner

- **Does:** runs a scenario end to end: the 13 startup steps ([operations](operations.md#startup-what-a-run-does-before-it-measures)),
  the measured window, the guardrails, the drain and the report; holds the pipeline lease (one run per
  pipeline).
- **How:** `runner/run.py` (`run_scenario`), `runner/flink.py` (job management, provenance, retained
  checkpoints), `runner/infra.py` (infrastructure sampling and the health watch), `runner/seed.py`.
- **Out:** the run directory: `manifest.json`, `run.json`, `startup.json`, `metrics.jsonl`,
  `infra.jsonl`, `run.log`, event and decision Parquet, `report.*`, `verify.json`.
- **Fails:** every outcome ends in the same teardown: children stopped, decisions drained, a report
  written, the lease released; an abort from anywhere is honoured within seconds.

## Verifier

- **Does:** proves the pipeline computed what it should: replays the run's event log through the
  Python twin and compares features, rules, score and outcome event by event.
- **Code:** `python/realy/verify`; the twin rule: every behaviour has a Java and a Python
  implementation (`EntityAggregator` ↔ `features/engine.py`, `DerivedFeatures` ↔ `features/derived.py`,
  `RuleSet` ↔ `policy.py`, `ProxyModel` ↔ `ProxyScorer`).

## Report, insights and what-if

- **Does:** turns a run into a verdict and an explanation: latency against the budget, sustainability,
  detection (stopped, $ paid, false positives, step-up fates), per-rule and per-typology performance,
  ranked insights, and what-ifs for other thresholds and rule sets.
- **Code:** `python/realy/report` (`report.py`, `insights.py`, `whatif.py`), `contracts/insights.yaml`.

## Console

- **Does:** launch and steer runs, watch any run live (or replay a past one), explore every event and
  entity, compare runs, read the docs.
- **How:** FastAPI backend (`python/realy/control`: worker, store, telemetry hub, explorer, replay,
  pipeline state) and a React UI (`ui/`). See the [console guide](console-guide.md).

## Kubernetes deployment

- **Does:** the same stack on a cluster: Kustomize base and components (Strimzi Kafka, Flink operator
  with JobManager HA, Redis, ClickHouse, gateway, model server, console, bench Jobs) and overlays (local
  kind, dev, GKE + L4).
- **Where:** `deploy/k8s`; [deployment-kubernetes.md](deployment-kubernetes.md).
