# Building your real-time pipeline

This page walks from this repository to a low-latency decision pipeline for your own events: how the
parts fit, how your messages get in, what has been proven on the bundled stack, and what is only
predicted for higher rates. Each claim names the run or test that proves it.

## How it fits together

| Layer | What you change | Where |
|---|---|---|
| **Contracts** | your messages, which entities each carries, the features, the rules, the model's inputs | `contracts/` ([schemas](dev/schemas.md), [features](dev/features.md), [transactions](dev/transactions.md)), `rules/` |
| **Services** | nothing, usually: the gateway, the Flink job and the scoring core read the contracts; new aggregations or functions are plugins | [extending](dev/extending.md) |
| **Topology** | how events travel to a decision: asynchronous through Kafka, synchronous over gRPC within a budget, Flink scoring inline, or your own scorer as a black box | [topologies](reference/topologies.md), [building a topology](dev/topologies.md) |
| **Infrastructure** | your Kafka, Redis (Cluster), model server, ClickHouse, with TLS and credentials | [real infrastructure](real-infrastructure.md) |
| **Proof** | a scenario: the load, the mix, the fraud, the budget; `--verify` recomputes every decision independently | [running benchmarks](running-benchmarks.md) |

The pipeline you deploy is the same code the benchmark measures; the instrument (generator, collector,
verifier, console) stays in staging and CI. See [from benchmark to production](production-pipeline.md).

## Your own flow of events

Three shapes, all declared in contracts:

1. **One message per decision** (a card authorisation): a binding (`contracts/events/<type>.yaml`) maps
   its fields to the entities; it is the pipeline's default type.
2. **Several messages that form one transaction** (initiation, payee check, authentication,
   submission...): a transaction contract assembles them into the scored event within a wait budget, and
   later stages can be decided again. [Transactions](dev/transactions.md).
3. **Messages that carry only some of the entities and are not decided** (logins, credential changes,
   card lifecycle, device telemetry): each folds into the features that list it, and payments read them.
   [Message types](dev/schemas.md#message-types-that-carry-a-subset-of-the-entities).

Synchronous decisions (a purchase acknowledged within its budget) use the gateway's gRPC API: every
stage has a deadline, a missed model deadline falls back to rules, and the audit record is durable
before the reply. [Latency budget](latency-budget.md#synchronous-decisions-a-purchase-answered-within-its-budget).

## What is proven

On this repository's compose stack, on one 4-vCPU / 15 GB host shared by every component:

| Claim | Evidence |
|---|---|
| Logins, profile changes, card lifecycle and device telemetry feed customer, card, device and IP features alongside payments; customer and card features stay exact | `m2-multi-type-inline`: 30,000 payments + 29,588 other messages, exact-set agreement 100%, device and IP features 98.4% or better (agreement-only, as for payments), rule / score / outcome parity 100%, audit 30,000 / 30,000 |
| The same through the asynchronous gateway (features read from the online store) | `m1-multi-type-smoke`: exact-set 99.42%, parity 100%, audit complete; the remainder is store lag (a login seconds before its payment) |
| The new signals reach rules | `rules-whatif` of `baseline-ext` on `m2`: fraud flagged 86.3% → 99.7%, false positives 3.73% → 3.82%. The signals are synthetic (the generator skews logins and telemetry around fraud), so this proves the path, not real-world lift |
| Rich payments: nested blocks inside `TransactionEvent` (card, account, merchant, device, payee, authentication, a basket of items, attributes), with filters, stream features, an entity key and rules on nested paths, through Flink | `n1-rich-inline` (73 features, 11 on nested paths): exact-set agreement 100%, rule / score / outcome parity 100%, audit 30,000 / 30,000. `n2-rich-smoke` through the gateway: exact-set 99.94%, parity 100%. Nested columns populated in ClickHouse (`card_info.status`, `items.sku`, `authentication.attempts`, the attributes map) |
| Entity status as change feeds folded into Flink state: a card blocked or reported stolen, an account frozen or on an AML hold, a payee listed as a mule, read by each payment as of its time with no lookup; Flink features that depend on a status (`gate:`) | `s1-status-inline` (83 features): exact-set agreement 100% with `card_blocked`, the account (computed in its customer's stage) and the gated device / merchant counts, rule / score / outcome parity 100%, audit complete. `s2-status-smoke` through the gateway: PASS, p99 125 ms, exact-set 99.95%, `payee_listed` 100% agreement from its own topic; `s3-status-smoke` after fixing the gateway's read-time approximation of gated counts: exact-set 99.93%, the gated device and merchant counts at 99.3% or better. A status outlives the 8-day state TTL (`StickyStateTest`); account features stay exact at parallelism 3 (`StatusJobTest`) |
| Status reaches rules | `rules-whatif` on `s1`: baseline-status against baseline-rich, fraud $ paid 48,653 → 38,426 (−21%), fraud stopped 86.4% → 88.1%, `CARD_BLOCKED` 237 fires at precision 1.0, false positives 4.39% → 4.63% (the AML-hold rule on simulated noise holds). The simulator stops genuine payments on a blocked card, so the precision proves the path, not real-world lift |
| Multi-message transactions assemble and decide exactly | `flows-inline`: exact 100% |
| Synchronous decisions within the budget | `sync-budget4`: 45,010 requests at 500/s, 100% within 150 ms, p99 51 ms |
| Real infrastructure: SASL Kafka, a 3-node Redis Cluster with a password, ClickHouse with a user | `r8-secured-smoke5`: p99 107 ms, verify PASS |
| Your own scorer as a black box | `r6-external`: 30,000 / 30,000 decided, 0 duplicates |
| Payments unchanged by all of this | the golden payments set and state bytes (`GoldenPaymentsTest`, `tests/test_golden_payments.py`); `n3-payments-smoke` on the payments pipeline after the nested blocks: PASS, e2e p99 111 ms with 99.82% within 150 ms, verify PASS (exact-set 99.80%, parity 100%); the flat payment path within ±3% in `HotPathBench`. `s4-payments-smoke` after the status work (the Flink stage hosting owned entities, sticky state, gates): PASS, p99 125 ms, verify exact-set 99.84%, parity 100%, CPU per payment as before (Flink 1.59 ms, gateway 1.07 ms) |

## What is not proven yet: throughput

| Rate | Status |
|---|---|
| 500 payments/s, payments only | within budget on this host on warm state (`m4-smoke-payments-warm` p99 147 ms, `sync-budget4` p99 51 ms); a fresh-state run whose feature catch-up overlaps the measured window does not (`m3`: p99 282 ms) |
| 500 payments/s + about 1 other message per payment | correct, **over budget**: e2e p99 300 ms (`m1`, `m2`). The host runs Kafka, Flink, Redis, ClickHouse, the gateway, the model and the load generator on 4 vCPUs |
| 500 rich payments/s (every block) | correct, **at the edge**: `n1-rich-inline` p99 152 ms (98.95% within 150 ms), `n2-rich-smoke` p99 170 ms; Flink inline at 2.3 ms CPU per payment with 73 features. The tail is the keyed shuffles on this shared host |
| 500 rich payments/s + status change feeds | gateway **within budget** (`s2-status-smoke` p99 125 ms, 99.53%; gateway 1.19 ms and Flink 1.53 ms CPU per payment); inline over (`s1` p99 173 ms). Status adds about 2% Flink CPU per payment (2.38 against 2.33 ms inline): the account rides in the customer stage, no extra shuffle |
| 1,000 payments/s | the latest step-ramp's knee; p99 241 ms with 4.8% rules-only fallbacks |
| 2,000 payments/s sustained | **not reached**. Measured CPU per payment at 1k: Flink 0.87 ms, gateway 0.66, emitter 0.30, model 0.22, collector 0.12, so about 4.35 cores at 2k for these five alone. The cost per event has to roughly halve, and the host's I/O (Kafka acks, checkpoint flushes, about 10.5 Redis operations per payment, ClickHouse merges) has to stop stalling |
| 20,000 payments/s | **predicted only**. Never run beyond one gateway, Flink parallelism 2 and 12 partitions. The GKE overlays exist; their sizing still uses assumed costs |

The plan for both is the next two phases:

- **2k sustained on this host:** profile the gateway and the TaskManager, a Java asynchronous emitter
  so the instrument is never the limit, fix the checkpoint stalls, coalesce Redis writes, tune Kafka and
  ClickHouse. Gate: `sustained-2k` (2k payments/s plus the message mix, 10 minutes, p99 ≤ 150 ms,
  ≥ 99% within budget, zero loss, verify 100%) and a step-ramp knee at 3k or more.
- **20k on GKE:** fix the overlay (HPA within partitions ÷ consumers, static membership, max
  parallelism 256, a 6-shard Redis Cluster), size every component from measured costs per message type
  (`pulsebench size --from <runs>`), prove linear scaling on this host (replicas 1 → 3, parallelism
  1 → 4), then `baseline-20k` and a step-ramp to 25k on a cluster, with every number marked predicted
  or proven.

## The rate you size for

The target counts decided payments per second; every other message is extra load. The report's
*message mix* line gives each type per decided payment (`m1`: 0.61 logins, 0.30 device reports, 0.05
card events, 0.03 profile changes). A login costs less than a payment in Flink (it skips the stages
that fold nothing for it and is dropped after its last one) and nothing at the gateway, which skips it.

## Checklist: from here to your environment

1. Bind your messages and declare your features in a pipeline of your own (`PIPELINE=<name>`); run
   `pulsebench bindings check`.
2. Pick the topology for each decision: asynchronous, synchronous within a budget, or inline.
3. Point the services at your infrastructure and run `pulsebench infra check`.
4. Write a scenario with your rates, your mix and your budget; run it with `--verify` on fresh state.
5. Raise the rate with a step-ramp until the knee; size from those measured costs.
6. Keep the scenario in CI (`pulsebench gate`) so every change is measured against it.
