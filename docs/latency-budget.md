# Latency budget: what is measured, and what the local runs showed

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
| 9 | Audit visibility | `ingested_at − decided_at` (ClickHouse) | ClickHouse |

## Local results (4 vCPU / 15 GB container, everything on one host)

All runs: zero missing decisions and zero duplicates unless stated. Budget: e2e p99 ≤ 150 ms, hot path p99 ≤ 100 ms.

| Run | Topology | TPS | e2e p50 | e2e p99 | hot-path p99 | Verdict | Notes |
|---|---|---|---|---|---|---|---|
| echo-calibration | echo (gateway answers immediately) | 2,000 | 4.5 ms | 30.6 ms | 0.6 ms | PASS | **instrument floor**: Kafka round trip + driver |
| smoke (warm) | gateway-kafka | 500 | 19.1 ms | 75.9 ms | 37.8 ms | PASS | tail: egress 78% |
| mixed-realistic | gateway-kafka + 1-day backfill, trained proxy | 500 | 19.8 ms | 107.3 ms | 44.6 ms | PASS | recall 0.99, precision 0.48, FPR 3.8%; `verify` PASS |
| flink-inline-smoke (fresh) | flink-inline | 500 | 23.1 ms | 99.4 ms | 70.6 ms | PASS | tail: **keyBy shuffles 61%**; features verified **exact** |
| sync-smoke | gateway-grpc (unary) | 500 | 18.9 ms | 96.4 ms | 45.7 ms | PASS | audit ack p99 39.8 ms = 27% of tail |
| sync-vs-async | gateway-grpc (bidi stream) | 500 | 14.7 ms | 43.5 ms | 32.7 ms | PASS | streaming halves the sync p99 |
| user-example-100ms | gateway-kafka, 100 ms feature build + 30–100 ms model | 1,000 | 170.8 ms | 592.6 ms | 162.4 ms | **FAIL (expected)** | 80.7% rules-only fallbacks; `realy budget` predicted INFEASIBLE / 75% |
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
   re-emit decayed values on a timer; measure again with `realy verify`. Flink-inline features are exact.
3. **The synchronous audit write is affordable but visible**: Kafka acks=all adds p99 ≈ 40 ms locally
   (27% of the sync tail). On dedicated brokers this is typically 2–5 ms; ClickHouse visibility is
   asynchronous (p50 ≈ 0.6 s, p99 ≈ 1.2 s at 500 TPS) and complete (0 gaps).
4. **Exactly-once costs a checkpoint interval** of end-to-end latency for read_committed consumers.
5. **Budget arithmetic**: a fixed 100 ms feature build plus a 30–100 ms model can never fit 150 ms;
   deadlines + rules-only fallback keep the system answering (81% degraded) but the budget fails.
6. **Cold state inflates false positives** (device/IP-country novelty): 16% FPR cold vs 3.8% with one
   simulated day of backfill and the trained proxy.
7. **Instrument hygiene changed the numbers**: batch-waiting / busy-polling collectors, 5 ms
   `fetch.max.wait.ms` churn, a JVM-spawning healthcheck and a userland docker proxy each inflated the
   measured tail before they were fixed — calibrate the instrument (echo run) before trusting results.
