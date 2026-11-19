# Capacity planning for 10,000+ TPS under a 100 ms hot-path / 150 ms end-to-end budget

Sizing is Little's law (`in-flight = arrival rate × time in system`) plus utilisation headroom: tail
latency stays flat only well below saturation (aim for ≤ 50–60% CPU on the hot path). Every number
below should be confirmed with `step-ramp` / `capacity-saturation` / `soak-60m` on the target cluster —
the instrument exists precisely because these estimates are where projects get surprised.

## Worked example: 10k TPS, 2× peak headroom (20k TPS design point)

| Component | Load at 20k TPS | Sizing rule | Starting point |
|---|---|---|---|
| Kafka `events.raw` | 20k msg/s × ~250 B = 5 MB/s | ≤ ~1–2k msg/s per partition for low-latency consumers; partitions ≥ max consumer parallelism | 48 partitions, RF 3, `min.insync.replicas=2` |
| Kafka `decisions` | 20k × ~1.5 KB = 30 MB/s (90 MB/s with RF 3) | broker NIC + disk; fat records (full inputs for audit) dominate | 48 partitions, 3 brokers × 4 vCPU |
| Flink features | 7 keyed stages / event (= 7 shuffles, 7 RocksDB read+write) | measure events/s per slot with `capacity`; keep checkpoint < 1/3 interval | parallelism 24 (6 TM × 4 slots), RocksDB on local NVMe |
| Flink async scoring (inline) | in-flight = 20k × 0.04 s = 800 | `async-capacity` per subtask ≥ in-flight / parallelism × 3 | capacity 1000, timeout 1 s |
| Gateway | in-flight = 20k × 0.04 s (p50 hot path) = 800; p99 ≈ 3× | ~0.3–0.5 ms CPU per decision → 20k × 0.4 ms = 8 cores busy | 6–8 pods × 4 vCPU (Guaranteed QoS), HPA at 55% CPU |
| Redis | gateway 9 GET + Flink 7 SET per event → 320k ops/s | single primary ~100–200k pipelined ops/s per core | Redis Cluster 3 shards (or io-threads 4 on a dedicated node) |
| Model (Triton, L4) | 20k inferences/s, batch-friendly | dynamic batching 0.5 ms → batches of ~10; an L4 runs the proxy MLP at ≫100k rows/s | 2 replicas × 1 L4 (HA), add replicas for the GBDT |
| Model (mock) | in-flight = 20k × 0.01 s = 200 | `workers` ≥ in-flight × 2 to stay unsaturated | workers 512 across 3 pods |
| ClickHouse | 20k rows/s, ~1.7 B rows/day | batched Kafka-engine inserts (~1 s), ≤ 1 insert/s/table | 8 vCPU, 500 GB SSD, `kafka_num_consumers` ≤ cores |
| Emitter (Python) | 20k ev/s | generator 100k+ ev/s/process; emitter ~15–25k ev/s/process | `traffic.generators: 2–4` |
| Collector (Python) | 20k decisions/s | ~15k/s/process (runner auto-scales by TPS) | 2 processes |

## Rules of thumb that matter for the tail

* **Kafka consumers**: `fetch.min.bytes=1` (respond on first byte); `fetch.max.wait.ms` only bounds
  empty-fetch churn (100 ms). Producers: `linger.ms` 1–5 with lz4 — fewer, larger requests improve
  p99 at high rates.
* **Flink**: `execution.buffer-timeout.interval` 1–5 ms (default 100 ms is a latency floor),
  AT_LEAST_ONCE checkpoints (no barrier alignment), incremental RocksDB checkpoints, unaligned
  checkpoints only for exactly-once, local SSD for RocksDB, `noWatermarks()` when event-time
  ordering is not needed for correctness.
* **Hot keys**: Kafka is keyed by customer (ordered per customer); merchants are the skewed entity
  (a large e-commerce merchant can be 5–10% of traffic) — watch `features:merchant` backpressure.
* **JVM**: ZGC (generational), `AlwaysPreTouch`, requests == limits (no CPU throttling), and never
  run the driver on the same nodes as the system under test.
* **Deadlines over waiting**: per-stage deadlines + rules-only fallback bound the p99 when a
  dependency degrades; the report counts degraded decisions so the cost is visible.

## How to validate a sizing

1. `realy budget baseline-10k` — the composition of your stage distributions must be feasible.
2. `realy run step-ramp` (gke-l4 overlay) — find the knee (TPS where p99 breaks or backlog grows).
3. `realy run baseline-10k` at the design point, then `soak-60m` for drift, lag and state growth.
4. `realy run capacity-saturation` / `tail-stress` / `black-friday` for capacity, tails and peaks.
