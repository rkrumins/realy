# Latency budget: what is measured, and what the local runs showed

## Synchronous decisions: a purchase answered within its budget

A card authorisation (or any caller that waits) uses the `gateway-grpc` topology. The caller sends the
payment to the gateway's Score API (gRPC unary, a bidirectional stream, or HTTP/JSON), and the gateway
answers with the decision. Everything on that path is bounded by the budget:

| Step | Bounded by | When it is late |
|---|---|---|
| the whole decision | `scoring.budget_ms` (100 ms in `sync-smoke`) from the moment the request arrives, or the caller's own deadline (`ScoreRequest.deadline_ms`; the emitter's `sync.deadline_ms`), whichever is given | `scoring.on_deadline`: `FALLBACK_RULES` (default) answers with a rules-only decision flagged degraded; `TIMEOUT` fails the request (`DEADLINE_EXCEEDED`); `PROVISIONAL_THEN_ASYNC` answers provisionally now and publishes the final decision when the model answers |
| feature lookup | `feature_deadline_ms` (30 ms): one pipelined Redis round trip for every entity's blob and profile. The features are precomputed by Flink off the decision path, so nothing is aggregated while the caller waits | the same deadline policy: a rules-only decision on the event, with the unread features at their defaults and marked missing, flagged degraded |
| model call | `model_deadline_ms` (80 ms), never past what is left of the budget; `hedge_after_ms` sends a duplicate request to a second model replica when the first is slow | rules-only fallback (above) |
| rules and policy | CEL compiled once; microseconds | n/a |
| the acknowledgement | the decision is made durable before the reply: the broker's ack (acks=all) on the decisions topic, or an fsynced local spool when the broker cannot take it (`audit_on_failure: SPOOL_LOCAL`, replayed later). `FAIL_CLOSED` declines a payment whose decision cannot be recorded | it counts against the budget like any other step (the `audit_ack` stage) |

The caller therefore always gets an answer inside its deadline, a degraded one at worst. The report
counts how many answers were degraded, why, and the response time of every request.

**What is measured.** The Java load emitter is open loop: each request is sent at its scheduled time
whether or not earlier ones were answered. A slow gateway therefore cannot slow the load down and hide
its own latency (coordinated omission). End to end is `t_response − t_intended` on the emitter's clock.
Requests that miss the deadline or fail are counted, never dropped.

**Measured** (`sync-smoke`, 500 TPS, unary gRPC, budget 100 ms in the gateway, 150 ms end to end, this
4-vCPU host, 2026-11-23):
- **Requests:** 45,010 answered, 0 missing.
- **Latency:** e2e p50 16 ms, p99 51 ms, p99.9 96 ms; hot path p99 35 ms; **100% within budget**.
- **Fallbacks:** 155 decisions (0.34%) were rules-only because the model missed its deadline.
- **Verify:** PASS, exact features 99.99%, rule, score and outcome parity 100%; audit 45,010 of 45,010.
- **Tail attribution:** the slowest 1% were dominated by the model call (36%), the response transit
  (27%) and the audit ack (21%).

`sync-vs-async` compares the unary rail with the bidirectional stream (streaming roughly halves the
p99) and with the asynchronous rail. Run either with `pulsebench run sync-smoke --verify`, or from the
console's Launch page.

## Per-step measurement

| # | Step | Span | Clock domain |
|---|---|---|---|
| 1 | On-time publishing | `t_sent − t_intended` | driver |
| 2 | Produce ack | librdkafka delivery latency | driver |
| 3 | Ingress transit + pickup | `t_ingest − t_sent` | cross (same host locally) |
| 4 | Flink keyBy shuffles / keyed state / async wait | per operator | Flink TM |
| 5 | Feature lookup, assemble, model call, rules, policy, audit ack | per stage | gateway / Flink |
| 6 | Model server queue, batch formation, compute, batch size | KServe v2 response parameters | model server |
| 7 | Egress transit (async) / response (sync) | `t_collected − t_decision` | cross |
| 8 | End to end | `t_collected − t_intended` (sync: `t_response − t_intended`) | driver |
| 9 | Audit append | `appended_at − decided_at`: broker LogAppendTime of the decision record (`lat_audit_append_us`, 1 ms resolution) | broker vs service |
| 10 | Audit visibility | `ingested_at − decided_at` (ClickHouse) | ClickHouse |

## Local results (4 vCPU / 15 GB container, everything on one host)

All runs: zero missing decisions and zero duplicates unless stated. Budget: e2e p99 ≤ 150 ms, hot path p99 ≤ 100 ms.

| Run | Topology | TPS | e2e p50 | e2e p99 | hot-path p99 | Verdict | Notes |
|---|---|---|---|---|---|---|---|
| echo-calibration | echo (gateway answers immediately) | 2,000 | 4.5 ms | 30.6 ms | 0.6 ms | PASS | **instrument floor**: Kafka round trip + driver |
| smoke (warm) | gateway-kafka | 500 | 19.1 ms | 75.9 ms | 37.8 ms | PASS | tail: egress 78% |
| mixed-realistic | gateway-kafka + 1-day backfill, trained proxy | 500 | 19.8 ms | 107.3 ms | 44.6 ms | PASS | recall 0.99, precision 0.48, FPR 3.8% **in-sample** (the model was trained on this world; see [fraud-detection.md](fraud-detection.md#training-and-evaluation) for out-of-sample numbers); `verify` PASS |
| fraud-eval, observed-label model | gateway-kafka + 1-day backfill | 500 | 43.7 ms | 262.2 ms | 93.8 ms | **FAIL** | host CPU-saturated: 94.5% within budget, 6.0% rules-only fallbacks; **out-of-sample** detection: recall 0.919 / 0.845 (flagged / declined), precision 0.31, FPR 4.1%; `verify` PASS |
| fraud-eval, ground-truth model | gateway-kafka + 1-day backfill | 500 | 17.3 ms | 111.4 ms | 44.4 ms | PASS | same scenario: 99.6% within budget, 0.7% fallbacks; out-of-sample: recall 0.965 / 0.947, precision 0.34, FPR 3.7% |
| fraud-train, legacy model | gateway-kafka + 1-day backfill | 500 | 20.8 ms | 157.7 ms | 59.1 ms | FAIL | 98.9% within budget (< 99%); 1.9% fallbacks |
| tail-stress, hedging off / on | gateway-kafka, slow-model tail | 500 | 24.4 / 24.4 ms | 92.6 / 87.0 ms | 63.2 / 50.7 ms | PASS | `hedge_after_ms = 30`: fallbacks 3.76% → 2.48%, model p99 60 → 43 ms, p99.9 167 → 141 ms |
| flink-inline-smoke (fresh) | flink-inline | 500 | 23.1 ms | 99.4 ms | 70.6 ms | PASS | tail: **keyBy shuffles 61%**; features verified **exact** |
| sync-smoke | gateway-grpc (unary) | 500 | 18.9 ms | 96.4 ms | 45.7 ms | PASS | audit ack p99 39.8 ms = 27% of tail |
| sync-vs-async | gateway-grpc (bidi stream) | 500 | 14.7 ms | 43.5 ms | 32.7 ms | PASS | streaming halves the sync p99 |
| user-example-100ms | gateway-kafka, 100 ms feature build + 30–100 ms model | 1,000 | 170.8 ms | 592.6 ms | 162.4 ms | **FAIL (expected)** | 80.7% rules-only fallbacks; `pulsebench budget` predicted INFEASIBLE / 75% |
| eos-cost | flink-inline, exactly-once sink | 500 | 5,319 ms | 10,187 ms | 61.1 ms | **FAIL (expected)** | decisions visible only at checkpoint commit (10 s) |
| step-ramp | gateway-kafka, 2M customers | 1k → 10k | — | — | — | FAIL | knee at 2k TPS; ceiling ≈ 5–6k decisions/s; guardrail abort at 10k |

### Latency vs throughput (step-ramp, this host)

| target TPS | decided/s (arrival) | e2e p50 | e2e p99 | hot p99 | model p99 | feature p99 |
|---|---|---|---|---|---|---|
| 1,000 | 1,000 | 38 ms | **123 ms** | 62 ms | 56 ms | 28 ms |
| 2,000 | 1,999 | 81 ms | 253 ms | 105 ms | 82 ms | 52 ms |
| 3,000 | 2,993 | 141 ms | 461 ms | 188 ms | 167 ms | 132 ms |
| 4,000 | 4,037 | 212 ms | 709 ms | 398 ms | 139 ms | 377 ms |
| 5,000 | 4,914 | 335 ms | 1,503 ms | 728 ms | 238 ms | 692 ms |
| 6,000 | 5,416 | 1.6 s | 5.9 s | 1.6 s | 294 ms | 1,085 ms |
| 8,000 | 5,788 | 17 s | 36 s | 2.0 s | 276 ms | 1,183 ms |

Every component (Kafka, 7-stage Flink with RocksDB, gateway, mock model, Redis, ClickHouse) **and**
the Python driver share 4 vCPUs, so this is a functional ceiling, not a capacity estimate. 1.42 M
events were all decided (no loss) even while the backlog grew; the run aborted itself when the
emitter's own schedule lag exceeded the 2 s guardrail. Run `step-ramp` / `baseline-10k` on the
`gke-l4` overlay for the real numbers (`docs/k8s-l4.md`).

## Findings that matter for the production design

1. **Flink-inline puts shuffles on the hot path**: 61% of the slowest 1% is time spent in the 7 keyBy
   network hops (buffer flushes, serialisation), although keyed-state work itself is ~0.5 ms p50.
2. **Topology B (gateway + online store) is faster; its windowed features were approximate and now
   are exact for customer/card.** *Update:* with state blobs (`contracts/storage/redis.md`) the gateway
   re-windows the stored aggregator state at each event's time: customer/card agreement **99.73%**
   (was ~85% / 82.6%), the remainder being store lag; shared values-only entities stay approximate
   (`merch_count_1m` 32.8%) - see docs/production.md. Original finding: The
   verifier finds customer/card features served from Redis agree with exact recomputation only
   ~85% overall and 32–50% for short windows (`card_count_1m`, `card_count_5m`): a blob holds the
   window *as of the entity's last event* and is not re-windowed when read later, so expired events
   are still counted. Fix options: store bucketed partial aggregates and re-window at read time, or
   re-emit decayed values on a timer; measure again with `pulsebench verify`. Flink-inline features are exact.
3. **The synchronous audit write is affordable but visible**: Kafka acks=all adds p99 ≈ 40 ms locally
   (27% of the sync tail). On dedicated brokers this is typically 2–5 ms; ClickHouse visibility is
   asynchronous (p50 ≈ 0.6 s, p99 ≈ 1.2 s at 500 TPS) and complete (0 gaps).
4. **Exactly-once costs a checkpoint interval** of end-to-end latency for read_committed consumers.
5. **Budget arithmetic**: a fixed 100 ms feature build plus a 30–100 ms model can never fit 150 ms;
   deadlines + rules-only fallback keep the system answering (81% degraded) but the budget fails.
6. **Cold state inflates false positives** (device/IP-country novelty): 16% FPR cold vs 3.8% with one
   simulated day of backfill and the trained proxy.
   State from **another world** is worse than cold: a different seed over warm Flink state served
   features only 78.8% exact and `IMPOSSIBLE_TRAVEL` fired on 4.4% of legitimate customers (FPR
   7.27%; rerun on clean state: 99.8% exact, FPR 1.31%). The runner
   now starts state fresh unless the running job's recorded world matches, and `verify` fails gateway runs below 98% exact-set
   agreement.
7. **Hedging trims the model tail cheaply:** duplicates on 2.8% of calls cut rules-only fallbacks by a
   third on `tail-stress`. On a saturated host, though, fallbacks follow CPU contention, not the model:
   the same evaluation scenario ran at 6.0% and 0.7% fallbacks (measured window) on two runs, and
   one of them failed the latency budget. Read the degraded share
   together with the host CPU in the report's resources section.
8. **Instrument hygiene changed the numbers**: batch-waiting / busy-polling collectors, 5 ms
   `fetch.max.wait.ms` churn, a JVM-spawning healthcheck and a userland docker proxy each inflated the
   measured tail before they were fixed — calibrate the instrument (echo run) before trusting results.
