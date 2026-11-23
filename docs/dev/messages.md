# Messages: what goes in and out, and how to change them

Every message the pipeline exchanges is a protobuf contract under `contracts/proto/`. The field-by-field
reference, generated from those files, is [reference/messages.md](../reference/messages.md); the topics
(key, headers, partitions, producers and consumers) are at its top. This page explains how the messages
fit together and how to change one safely.

## The flow

| Step | Message | Where |
|---|---|---|
| A payment enters | `TransactionEvent` | Kafka `payments.events.raw`, key = customer id; or the gateway's gRPC `ScoringService/Score` (sync rail) |
| Benchmark timing travels with it | `BenchTrace` | Kafka header `x-bench-trace` (async) or the `trace` field (sync): run id, intended and sent time, clock domain, spans |
| A decision leaves | `DecisionRecord` | Kafka `payments.decisions`: outcome, score, path, rule hits, reason codes, the event, the feature vector (sampled), freshness per entity, every span |
| The truth arrives later | `FraudLabel` | Kafka `payments.labels`, at the label's simulated time (chargeback, customer report) |
| Configuration | `ScoringConfig`, `ModelServerConfig` | Kafka `bench.control` (compacted; key `<target>`) and the services' `POST /admin/config` |
| Telemetry | `MetricsSnapshot` | Kafka `bench.metrics`, every second from every service |
| Model inference | KServe v2 `ModelInfer` | gRPC to the model server: one FP32 tensor `[batch, n_features]` in feature-schema order ([models](../feature-store-and-inference.md)) |

The headers on `payments.events.raw` are part of the contract: `x-run-id` (the owning run; `:backfill` and
`:prime` suffixes for history), `x-backfill` (history: Flink folds it into state, the gateway does not
score it), `x-mirrored` (a sync decision mirrored so Flink updates state) and `x-bench-trace`.

## Compatibility rules

- **Fields are appended, never removed, renumbered or retyped.** `python/tests/test_proto_compat.py`
  compares every message and enum with the committed snapshot `contracts/proto/compat.lock.json` and
  fails on a breaking change. A field you no longer need is left in place (or marked `reserved`).
- **Readers tolerate absent fields.** proto3 reads a missing field as its zero value, so an old event
  log still decodes; code that must tell "absent" from "zero" uses an explicit default (the Python
  `event_view` fills `LATER_FIELDS` for events written before a field existed).
- **Names matter beyond the wire**: ClickHouse columns and proto3 JSON (configs on `bench.control`) use
  them, so a rename is treated as breaking too.

## Walkthrough: add a field to `TransactionEvent`

Say the issuer starts sending `bool card_on_file` (a stored credential, for recurring and one-click
payments) and you want rules and features to use it.

1. **Contract.** Append it to `contracts/proto/realy/payments/v1/transaction.proto` with the next free
   number and a comment:
   ```proto
   bool card_on_file = 33;   // credential stored by the merchant (recurring, one-click)
   ```
2. **Stubs.** Java regenerates on build (`java/contracts-java`); Python stubs are committed:
   `scripts/gen-proto-python.sh`.
3. **Snapshot.** `python -m realy.proto_compat --update` (the compatibility test fails until you do, so
   additions are deliberate), then commit `contracts/proto/compat.lock.json`.
4. **Both event views.** Rules and expression features read events through a view, mirrored in Java
   and Python and kept equal by `verify`: add the field to `EventView.java`
   (`java/scoring-core/.../features/EventView.java`) and to `event_view` in
   `python/realy/features/derived.py` (and to `LATER_FIELDS` with its default, so older event logs read it
   as `false`).
5. **The generator** fills it: the batch columns in `python/realy/gen/batch.py` (and the behaviour or
   typology that sets it, e.g. recurring payments) and `finalize` writes it into the event.
6. **The warehouse.** Add `("card_on_file", "UInt8")` to `EVENT_COLS` in `python/realy/codegen.py`,
   run `make codegen`, then `make migrate` on a running stack: the events table and the decisions table
   gain the column in place, and the Kafka queue and view are rebuilt (they hold no data; the consumer
   group keeps its offsets).
7. **Use it.** A rule (`rules/*.yaml`, `event.card_on_file`) or an expression feature
   (`feature_schema.yaml`, `fn: expr`); see [rules and policy](../fraud-detection.md) and the
   [feature guide](../feature-store-and-inference.md).
8. **Docs.** `make docs` regenerates [reference/messages.md](../reference/messages.md); the docs
   staleness test fails if you forget.
9. **Prove it.** Python tests (`pytest`), Java tests (`./gradlew test`), then a verified run
   (`pulsebench run smoke --verify`): rules, features, score and outcome must agree 100% between the
   pipeline and the Python twin.

## Walkthrough: consume decisions from another service

Subscribe to `payments.decisions` with your own consumer group and decode `DecisionRecord`
(`realy.scoring.v1`). The key is the customer id, so one customer's decisions stay ordered within a
partition. A decision can be revised (`revision` > 0: a provisional rules-only decision followed by the
model's final one when hedging is on); keep the highest revision per `event_id`. The record carries the
event itself, so a consumer needs no join.

## Your own Kafka (TLS / SASL)

Every client of this project (Python services and runner, the Java gateway, model server and metrics,
the Flink source and sink) reads the same environment variables: `KAFKA_BOOTSTRAP`,
`KAFKA_SECURITY_PROTOCOL` (`SSL`, `SASL_SSL`, ...), `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME` /
`KAFKA_SASL_PASSWORD` (or `KAFKA_SASL_JAAS_CONFIG` for Java), and the trust / key stores
(`KAFKA_SSL_CA_LOCATION` for librdkafka, `KAFKA_SSL_TRUSTSTORE_LOCATION` / `KAFKA_SSL_KEYSTORE_LOCATION`
for Java). Unset means PLAINTEXT, as in the compose stack. Sources: `SECURITY_ENV` in
`python/realy/common/kafka.py`, `KafkaProps.security()` in `java/common`.
