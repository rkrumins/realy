# Productionisation and capacity sizing

This guide turns what PulseBench measured into recommendations for two things:
- **The production fraud pipeline:** Kafka → Flink → Redis → model → decisions → ClickHouse.
- **PulseBench itself, as a shared benchmarking platform:** a sizing method and a starting plan for
  **10,000 TPS steady / 20,000 TPS peak on GKE with L4 GPUs**.

Every recommendation cites the measurement behind it. Re-measure on your own hardware before relying
on any number; that is what the tools below are for.

## Fraud pipeline

### 1. Serve exact features from the online store

**Finding.** With Redis holding feature *values*, the gateway served customer/card features that
matched an exact recomputation only **82.6%** of the time.
- A blob holds values as of the entity's last event, and they are never re-windowed. A 10-minute
  count read 25 minutes later still counts expired events.
- The derived `credit_utilisation_24h` inherits the error from stale card sums.

**Fix: state blobs.** For customer and card, Flink now also writes the entity's aggregator state,
and the gateway re-windows it at each event's own time. That is the same code and the same
update-then-read semantics as the Flink-inline path (`contracts/storage/redis.md`).
- **Residual error:** store lag, meaning events of the same entity not yet written when the gateway
  reads. It is reported by `pulsebench verify`.
- **Measured** (`mixed-realistic`, all nine fraud typologies, 1 simulated day of history): served
  features agree with the exact recomputation **99.73%** of the time, up from **82.6%**.
  - verify PASS, with rule, score and outcome parity at 100%;
  - e2e p99 97 ms, unchanged within noise.
- **Cost:** customer/card blobs grow from about 80 B to roughly 0.3–1.5 KB each. The sizing below
  includes the extra Redis network and memory.
- **Shared entities** (merchant, device, IP, terminal, payee) stay values-only by default.
  - **Measured:** their short windows are noticeably stale. `merch_count_1m` agreed only 32.8% of the
    time, `term_count_5m` 56.5% and `dev_count_5m` 76%.
  - If the model leans on them, add them to the state set (`--store-state
    customer,card,merchant,terminal`, or `FLINK_STORE_STATE`), measure the extra Redis bandwidth with
    the report's `resources` section, and re-run `pulsebench size`.
  - Hot merchants have the largest states (about 2.5 KB) and the most writes, so this is a measured
    trade-off, not a default.

### 2. Topology for the hot path

Use the **gateway with the online store** for synchronous and low-latency asynchronous scoring.
- **Flink-inline** is exact by construction, but **61%** of its slowest 1% is the seven `keyBy`
  network shuffles.
- Keep it for near-real-time paths where an extra 50–100 ms is acceptable, and as the reference that
  `pulsebench verify` compares against.

### 3. Kafka

- **Durability:** RF 3, `min.insync.replicas=2`, `acks=all`, rack-aware across 3 zones.
  - With 3 brokers we measured **zero lost decisions** while a broker was stopped mid-run, with the
    ISR fully recovered about 5 s after it returned.
- **Dedicated nodes.** Under CPU contention, produce-ack p50 rose about **6×** (2.6 ms to 15 ms) and
  the latency budget failed while correctness held.
- **Partitions:** 48–60 for events and decisions (sized below). Keep partitions ≥ the largest
  consumer parallelism.
- **The decisions topic dominates storage.** Full input vectors make records about 1.5 KB, which at
  10k TPS is about **1.3 TB/day before replication**.
  - Set `feature_sample_permille` below 1000 for the Kafka copy, or move full vectors to a separate
    audit topic with its own retention.
  - Use tiered storage for anything longer than a day.

### 4. Flink

- **Platform:**
  - Run under the Flink Kubernetes Operator with Kubernetes HA, so a JobManager failure resumes from
    the last checkpoint.
  - Keep RocksDB on local NVMe and checkpoints on `gs://` (incremental, every 10–30 s).
  - Set `max-parallelism` ≥ 4× parallelism, so you can rescale later without losing state.
- **Delivery semantics:** at-least-once checkpoints with idempotent consumers. Decisions are
  de-duplicated by `(run_id, event_id)`, and ClickHouse collapses duplicates.
  - Exactly-once costs a checkpoint interval of end-to-end latency for `read_committed` readers:
    **5–10 s** measured with the `eos-cost` scenario.
- **Upgrades:** stop-with-savepoint then restore, with operator uids fixed (`features-<entity>`).
- **Bootstrap state before go-live:** cold state produced a **16%** false-positive rate, against
  **3.8%** after one simulated day of history.
  - Replay history through the job, or build the state with the State Processor API.
  - Then shadow production before taking decisions.

### 5. Redis (online store)

- **Topology:** Memorystore for Redis Cluster (or Redis Cluster), one replica per shard.
- **Shard count:** driven by the largest of operations (about 16 per event), network (state blobs)
  and memory (sized below).
- **Persistence is optional.** The store can be rebuilt from Flink state; document and rehearse:
  1. clear `fs:*`;
  2. restart the features job from its latest savepoint;
  3. let it write as entities appear, or replay history.
- **Watch** `store_update_lag` (Kafka pickup to Redis write). It bounds how exact gateway features
  can be.

### 6. Model serving (Triton on L4)

- **Configuration:**
  - dynamic batching (0.5–2 ms `max_queue_delay`);
  - two instance groups per GPU, with warm-up enabled;
  - at least 2 replicas across zones, sized so that one replica down still leaves headroom.
- **Find the batching knee on the real GPUs** with `capacity-saturation`. The mock's capacity model
  reproduces the shape, not your numbers.
- **Deadlines:** keep the 80 ms model deadline with the rules-only fallback. `tail-stress` showed the
  deadlines protecting the p99 when the model degrades, and reports count every degraded decision.
  - **A fallback costs recall:** rules alone flagged 32–42% of the fraud they saw in the evaluation
    runs, against 92–97% with the model. Budget the degraded share as a detection SLO, not only a
    latency one.
- **Hedging** (`ScoringConfig.hedge_after_ms`): a duplicate request on another channel when the first
  has not answered, first answer wins. Measured on `tail-stress` with `hedge_after_ms = 30`:

  | | off | on |
  |---|---|---|
  | rules-only fallbacks | 3.76% | **2.48%** |
  | model-call p99 | 60.1 ms | 43.3 ms |
  | hot-path p99 | 63.2 ms | 50.7 ms |
  | e2e p99 / p99.9 | 92.6 / 166.9 ms | 87.0 / 140.8 ms |
  | within the 150 ms budget | 99.85% | 99.93% |

  It sent 1,244 duplicates (2.8% of 45,010 calls), and 496 of them won (40%). Set it near the model-call p99 of a
  healthy run, so that only the slow tail is duplicated; size Triton for that extra load.

### 6a. Models, labels and evaluation

Measured with `make eval`: train on one synthetic world (fraud-train, seed 7), evaluate on another
(fraud-eval, seed 1337). Details: [fraud-detection.md](fraud-detection.md#training-and-evaluation).

- **Train on the labels you will actually have.** With a 30-day cut, only 82.6% of true fraud was
  labelled. Out of sample, that model declined 0.86 of fraud against 0.95 for a ground-truth model
  (same traffic), approving $171k of fraud against $56k.
  - **Let labels mature:** a 60-day cut labelled 88.8% and halved the fraud approved ($86k). Train
    on a window that ends 60+ days ago, or drop recent rows rather than learning them as legitimate.
  - **Slow-labelled typologies suffer most:** card testing, ATO, merchant breach and bust-out
    (chargebacks, investigations).
- **Gate a model on data it has never seen.** Reports compare the served model's training provenance
  with the run's world and print in-sample, out-of-sample or unknown. Promote on the out-of-sample
  number only.
- **Add decision-history features.** In the worked card-testing example, the cash-outs 20–36 minutes
  after the declined tests scored about 0, because no feature records that the card was just declined.
- **Prune rules by measured precision** (`realy.v_rule_performance`, `pulsebench rules-whatif`):
  `baseline-v3` removed one rule with precision 0.036 and halved the review queue (FPR 4.13% → 1.79%)
  for 0.3 points of flagged recall.
- **Shadow before promoting:** `pulsebench deploy-model` hot-swaps the mock's weights. With Triton,
  publish a new version to the model repository and compare both on mirrored traffic.

### 7. Scoring gateway

- **Resources:** Guaranteed QoS (requests == limits) and ZGC.
  - Two 3.5-vCPU pods per c3-standard-8. A 4-vCPU pod doesn't fit twice once GKE's reservations are
    taken.
- **Autoscaling:** HPA at about 55% CPU with a long scale-down stabilisation. Spread pods across zones.
- **Sync path:** the audit write must be acknowledged before the reply. If Kafka is unavailable, the
  record is spooled to local disk (`SPOOL_LOCAL`) instead of blocking the payment.
- **Benchmarks:** pin replicas (min = max). An autoscale mid-run causes consumer-group rebalances
  that show up as latency.

### 8. SLOs, alerts and audit

- **SLOs:** e2e p99 ≤ 150 ms, hot-path p99 ≤ 100 ms, and a degraded-fraction budget (for example
  ≤ 0.5% rules-only decisions).
- **Detection SLOs, once labels mature:** recall and precision per typology, fraud dollars approved,
  and the legitimate-customer review rate (FPR), from `realy.v_rule_performance` and the labels.
- **Alert on:**
  - consumer lag (gateway, Flink, ClickHouse) and `store_update_lag` p99;
  - checkpoint duration over one third of the interval;
  - Triton queue time and GPU duty cycle;
  - audit gaps: run the `realy.v_audit_gaps` view daily and page on any row;
  - audit append p99 (`lat_audit_append_us`; 39.6 ms locally, 2–5 ms expected on dedicated brokers);
  - Flink checkpoint failures (all 21 of 21 completed locally: RocksDB incremental, 118 MB state,
    p50 327 ms).

### 9. Security and data governance (real data)

- **Transport and access:**
  - Kafka TLS + SASL with per-service ACLs;
  - Redis AUTH + TLS;
  - ClickHouse users with least privilege;
  - secrets in Secret Manager, mounted via the Secrets Store CSI driver.
- **ClickHouse data:** TTL for retention, CMEK encryption at rest, and column-level access for PII.
- **Pods and images:** non-root containers with read-only root filesystems, NetworkPolicies
  (default deny), signed images pinned by digest.
- **Console:** behind IAP/SSO ([security.md](security.md)).

### 10. Rollout

1. **Shadow:** mirror production events to the new pipeline and compare its decisions with the
   current one offline. The console's Compare page and `pulsebench gate` work on any two runs.
2. **Canary** by merchant or BIN range.
3. **Full** cut-over, with `pulsebench gate` on every release (below).

## PulseBench as a platform

| Practice | Why |
|---|---|
| A dedicated `realy-driver` node pool | the instrument must not compete with what it measures; one host shared by everything saturated at 4 vCPUs |
| `echo-calibration` on every new cluster | records the instrument floor (Kafka round trip plus driver) next to the results |
| Nightly `baseline-10k`, `soak-60m` and `step-ramp` on gke-l4, gated | regressions show up within a day, with the run that introduced them |
| `pulsebench gate` in CI and on release | fails on loss, verify, p99, knee, detection or CPU-per-event regressions against committed baselines (`baselines/`) |
| Run history in ClickHouse (`realy.runs`) + Grafana `PulseBench - run history` | trends of p99, knee, CPU per event, detection and feature exactness per scenario |
| Console behind SSO, one per environment | it can start load against whatever it points at |
| Known gap: operator-mode Flink | in the operators layout the console observes FlinkDeployments but cannot switch them; drive the CRs from the console through the Kubernetes API (follow-up) |

## Capacity sizing

### Method

1. **Measure cost per event on the target machine types.**
   - Every run now records CPU per event for each component (gateway, Flink TaskManagers, model
     server, driver) plus throttling and heap, in the report's `resources` section.
   - It also samples Redis operations, CPU and network, Kafka message rates, and Flink busy and
     backpressure time (`infra.jsonl`).
2. **Work out the capacity:**
   - cores = peak TPS × cost / target utilisation (55%);
   - the in-flight count per replica follows from Little's law;
   - queueing comes from Erlang C for the CPU-bound gateway and from a dynamic-batching model for
     the GPU.
3. **Add redundancy:** N+1 per tier, rack-aware Kafka in multiples of the zone count, and one spare
   TaskManager.
4. **Check latency:** compose the critical path from the measured stage distributions with the Monte
   Carlo budget checker, plus queueing at the target utilisation.
5. **Confirm on the cluster:** `step-ramp` (find the knee) → `baseline-10k` at the design point →
   `soak-60m` (drift, lag, state growth).

**Calibrate at load.** At low rates, fixed per-process costs (poll loops, metrics threads, GC) are
spread over few events and overstate CPU per event.
- On this 4-vCPU development host at 500 TPS, the Python emitter measured 0.5 ms/event, while its
  real capacity is 15–25k events/s per process.
- Calibrate from a `step-ramp` that reaches the design rate. `pulsebench size --from` then uses each
  component's cost at the highest load level before the knee, and warns when a cost was measured
  below 2,000 events/s.

`pulsebench size` implements steps 1–4:

```bash
pulsebench size --tps 10000 --peak 2                          # documented assumptions (starting point)
pulsebench size --tps 10000 --peak 2 --from <step-ramp run>   # costs measured on your nodes
pulsebench size ... --set flink_cpu_ms=0.8                    # override a single cost
pulsebench size ... --write-overlay deploy/k8s/overlays/sized # kustomize patches on top of gke-l4
```

Every cost in its output is tagged `measured`, `assumed` or `override`. Warnings appear when a
measured component ran above 80% CPU or was throttled, since contention inflates cost per event.

### Starting plan: 10k steady / 20k peak, GKE + L4 (assumed costs)

Generated by `pulsebench size --tps 10000 --peak 2`. Replace with `--from` a run on your nodes.

| Tier | Node pool / machine | Nodes | Pods | Sizing |
|---|---|---|---|---|
| Scoring gateway | `realy-serving` / c3-standard-8 | 3 | 6 × 3.5 vCPU | 14.5 cores busy at 0.4 ms CPU/decision; HPA 6–12; CPU queueing p99 < 1 ms |
| Flink feature job | `realy-flink` / n2-highmem-8 (local NVMe) | 8 | 7 TaskManagers + 1 spare, 7 slots each | 43.6 cores at 1.2 ms/event; parallelism 49, max-parallelism 256; RocksDB state ~31 GB for 10M customers (~4.4 GB per TaskManager) |
| Kafka | `realy-kafka` / n2-standard-8 + pd-ssd | 3 | 3 brokers | 35 MB/s produced, ~250 MB/s cluster traffic with RF 3 and 5 consumer groups; 60 partitions; **~2 TB per broker** for 6 h events + 24 h decisions retention |
| Redis | Memorystore Redis Cluster | 6 shards (+6 replicas) | – | 320k ops/s, 110 MB/s, 40 GB (customer/card as state blobs) |
| Model | `realy-gpu` / g2-standard-8 (1 × L4) | 2 | 2 Triton (min 2, max 4) | 20k rows/s; 40% of one GPU with one replica down; dynamic batches of ~33 rows / 1.7 ms; queueing p99 ~2.7 ms |
| ClickHouse | `realy-clickhouse` / n2-standard-8 | 2 | 2 replicas + 3 Keeper | 40k rows/s inserted; **252 GB/day** compressed, so ~10 TB per replica at 30 days of retention |
| Bench driver | `realy-driver` / c3-standard-22 | 1 | 1 console | 2 generators + 2 collectors (benchmarking only) |

**Production total:** 18 nodes (144 vCPU), 2 L4 GPUs and 6 Redis shards, plus one driver node for
benchmarks. The predicted latency at 20k TPS is e2e p99 ~84 ms and hot path ~35 ms, which is
**FEASIBLE** against 150 / 100 ms, using the default infrastructure baselines plus queueing.

**What the sizing teaches.**
- **Storage, not CPU, is the expensive tier.** Most of the bytes are full feature vectors on every
  decision. Sampling them (`feature_sample_permille`), shortening decision retention in Kafka, and a
  ClickHouse TTL or tiering policy cut Kafka disk and ClickHouse storage several-fold.
- **Flink is the largest CPU consumer:** 7 keyed stages, each with state I/O and a shuffle. Its
  1.2 ms/event is an *assumption*. Measure it first. It is also where co-locating entity stages, for
  example customer and card in one keyed operator, saves the most.
- **One L4 is plenty for the proxy models at 20k TPS.** The second GPU is for availability. Real
  production models change this, so re-measure with them on Triton.
- **Gateway CPU is modest; its latency comes from its dependencies.** That is why per-stage
  deadlines matter more than more replicas.

**Fix carried out while sizing.** `overlays/gke-l4` requested 8 CPUs for Triton on a g2-standard-8
(8 vCPU). After GKE's reservations and the GPU DaemonSets that can never schedule, so it is now 6.

## Regression gating

```bash
pulsebench gate <run> --save-baseline baselines/smoke.json         # adopt a known-good run
pulsebench gate <run> --baseline baselines/smoke.json               # exit 1 on regression
pulsebench gate <run> --baseline <other run> --json                 # machine-readable
```

**Absolute checks:**
- the verdict, honouring scenarios with `expect: FAIL`;
- zero missing decisions, and duplicates within the allowance;
- verify passed.

**Against the baseline**, it fails when any of these regress:
- e2e and hot-path p99, beyond **both** +10% and +5 ms;
- the share of events within budget;
- knee TPS (−10%) and sustainability;
- recall and precision drops, or a false-positive-rate rise;
- CPU per event for the gateway and Flink (+15%);
- feature exactness.

CI runs the gate after the end-to-end job with wide latency allowances, because hosted runners are
noisy. Gate on dedicated nodes with the defaults.

Every run is also written to `realy.runs` in ClickHouse; for the Grafana dashboard, use `make up-obs`.
