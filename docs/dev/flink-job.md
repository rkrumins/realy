# The Flink job: how it works, how it fails, how to change it

The Flink job computes the stream features: it keeps a sliding window of state per customer, card,
device, IP, merchant, terminal and payee, writes each entity's features to the online store (Redis)
for the gateway, and in the `inline` topology scores the payment itself. This page explains the job
graph, its state, its fault-tolerance guarantees, how to read its checkpoint and backpressure numbers,
and how to change it. The feature semantics live in [the feature store guide](../feature-store-and-inference.md);
the feature DSL in [`contracts/features/feature_schema.yaml`](../../contracts/features/feature_schema.yaml).

Source: `java/flink-job/src/main/java/io/realy/flink/` (`FraudJob`, `EntityFeatureFunction`,
`StoreWriter`, `EventDeserializer`, `Envelope`, `ScoringAsyncFunction`), the shared aggregation code
in `java/scoring-core/.../features/EntityAggregator.java`, and the runner's job management in
`python/realy/runner/flink.py`.

## Job graph

```
payments.events.raw ─▶ source (uid source) ─▶ features:customer ─▶ features:card ─▶ features:device ─▶
   features:ip ─▶ features:merchant ─▶ features:terminal ─▶ features:payee ─▶
      features mode:  discard sink
      inline / both:  skip-backfill ─▶ score (async I/O, uid scoring) ─▶ payments.decisions (uid decisions-sink)
```

- **One keyed stage per entity** (`keyBy(entity id)` then `EntityFeatureFunction`, uid `features-<entity>`).
  Each `keyBy` is a network shuffle, so every event visits each entity's owner in turn and the stage
  sees that entity's events in order. Events without the entity (a transfer has no card) are spread by
  event id instead of piling on key 0, and get the defaults.
- **The source** parses the protobuf event once, decodes what the stages need into an `EventRow`
  (time, location, the key of every entity, and one slot per event field a stream feature reads, laid
  out by the schema's `RowPlan`), evaluates the CEL feature filters once (`filterMask`) and wraps it all
  in an `Envelope` that travels through the stages: the raw event bytes, the row, the filter mask, the
  feature vector being filled and, in `inline` / `both`, the benchmark spans. The keyed stages read the
  row and never parse the event again; only the scoring operator parses it (once) for the decision.
- **One schema per job.** The job's main resolves the feature schema (`FEATURE_SCHEMA`, else the
  bundled copy) and ships its text to every operator in the graph, so a TaskManager never resolves its
  own copy and cannot run a different schema than the one the graph was planned with.
- **Modes** (`--mode`): `features` (the gateway topologies: features written to the store), `inline`
  (Flink scores; no store writes), `both` (scores and writes), `echo` (instrument calibration). The
  runner starts the mode the scenario's topology needs and switches with stop-with-savepoint, so the
  keyed state carries over (the uids are the same in every mode).

## State

Each entity stage holds one RocksDB `ValueState<byte[]>` per key: the entity's window buckets
(count / sum / max / avg per granularity slot), its distinct / novelty sets with last-seen times,
and its last event time and position. TTL is `STATE_TTL_DAYS` (8 days, cleaned up in RocksDB
compaction).

**State encoding.** The blob is written in the compact v2 layout (varints, delta-coded bucket
indices and keys; integral values as varints and others as raw doubles, so every value round-trips
exactly). v1 blobs (earlier releases, state restored from old savepoints) still decode, and are
rewritten as v2 on their next update. Measured on realistic entity histories: **551 B → 161 B per blob
on average** (`StateCodecTest`). The same bytes are the online store's state blobs for customer and
card (`--store-state`), so the size reduction applies to RocksDB I/O, checkpoint size, Redis memory
and the gateway's read bytes per event alike. Upgrade the gateway and the Flink job together: an older
gateway reads a v2 blob as fresh state.

**Changing the schema.** Features are appended, never inserted (see the feature guide). A changed
number of features per entity resets that entity's state at its next event; the runner detects the
change through the state layout hash and starts fresh state warmed with history.

## Fault tolerance

What the job guarantees, and where it is set:

| Concern | Behaviour | Set in |
|---|---|---|
| Checkpoints | every 10 s (`--checkpoint-ms`), incremental RocksDB, at-least-once (no barrier alignment, no checkpoint-induced stalls); exactly-once with `--exactly-once true` (unaligned checkpoints, transactional sink) | `FraudJob.configure`, cluster config |
| A slow checkpoint | tolerated: 3 failed checkpoints in a row before the job fails over (`--tolerable-checkpoint-failures`); timeout 120 s (`--checkpoint-timeout-ms`) | `FraudJob.configure` |
| Retained checkpoints | the newest 2 are kept when the job is cancelled or lost (`RETAIN_ON_CANCELLATION`) | job + cluster config |
| A lost job | the runner restores a job that is gone without a savepoint (JobManager restarted, job cancelled by hand) from its newest retained checkpoint, with its state lineage | `runner/flink.py` (`latest_retained_checkpoint`) |
| Task failures | restart with exponential backoff (1 s → 60 s, jitter 0.1, reset after 10 min), forever: a dependency outage must not end the job | cluster config (`restart-strategy.*`) |
| TaskManager out of memory | the JVM exits (`taskmanager.jvm-exit-on-oom`) and the container restarts (`restart: unless-stopped`); a TaskManager that hangs anyway fails its health check (registered with the JobManager?) | compose / Kubernetes |
| JobManager loss | Kubernetes: JobManager HA (`high-availability.type: kubernetes`) recovers the job and its checkpoints. Compose (no HA): the runner restores from the retained checkpoint | Kubernetes manifests, runner |
| Online-store writes | bounded in flight (`STORE_MAX_IN_FLIGHT`, 1000 per subtask: a slow Redis backpressures the job instead of filling the heap); **drained before every checkpoint completes**, so a completed checkpoint means Redis holds every write for the events before it; a timed-out write is retried once (SET is idempotent), a write that still failed declines the checkpoint; a store failing for `STORE_FAIL_AFTER_MS` (30 s) fails the task, and the restart replays from the last checkpoint once the store is back | `StoreWriter` |
| The Redis client | rejects commands while disconnected, bounds its queue (`REDIS_REQUEST_QUEUE`) and times commands out (`REDIS_COMMAND_TIMEOUT_MS`: 2 s for the gateway, 10 s for the Flink writer, whose writes are off the decision path): an outage becomes errors, never heap growth | `RedisOnlineStore` |
| A full store | Redis runs `noeviction`: writes fail loudly instead of evicting feature state; the runner's health watch aborts the run at 97% of `maxmemory` with the reason (warns at 85%) | compose / Kubernetes, `runner/infra.py` |
| A store that lost its data | the health watch aborts the run when Redis restarts or its profiles vanish mid-run (its features would be wrong from there) | `runner/infra.py` |
| Rescaling | max parallelism fixed at 128 (`--max-parallelism`): the number of key groups, the upper bound for rescaling keyed state; changing it makes old savepoints unrestorable | `FraudJob.configure` |

**Sizing the in-flight bound.** Across all subtasks the bound is also the depth of Redis's queue, so it
must stay well below Redis's throughput × the command timeout. Measured: 5000 per subtask × 14 subtasks
during a bulk replay (~40k writes/s) queued for more than 2 s, writes timed out and checkpoints were
declined; 1000 per subtask keeps the queue around a third of a second and every checkpoint completed.

Every Flink job restart during a run is recorded in the run's manifest (`health.flink_restarts`,
`health.flink_last_failure`) and logged with its cause.

### What happened when checkpoints failed (and why it cannot happen the same way now)

Measured on the 4-vCPU / 15 GB host with `step-ramp` on the 2M-customer world:

1. The host ran out of memory (every service shares it; Redis held 3.2 GB of state blobs) and the
   kernel killed Redis. Redis had no restart policy, so it stayed down.
2. The Flink job's Redis client queued every write while disconnected (Lettuce's unbounded default),
   the TaskManager heap filled, the JVM stalled and missed its 50 s heartbeat, then died of
   `OutOfMemoryError` without exiting (the container stayed "Up" serving nothing).
3. The JobManager failed every task; restarts found no slots (`NoResourceAvailableException`) and the
   in-flight checkpoint failed with "Checkpoint Coordinator is suspending".

Each link is closed: Redis restarts and no longer outgrows its cap silently (`noeviction`, compact
state at a third of the size), the client fails fast, writes are bounded and checkpoint-aligned, the
TaskManager exits on OOM and restarts, and the run ends with the reason instead of a mystery.

## Reading checkpoints and backpressure

The console's **Environment** page lists each vertex with its parallelism (`×2`), backpressure level
(`bp ok / low / high`) and records read, and each job's checkpoint counts and last duration / size.

- **`bp high` on the source and the first stages during the history replay is expected.** The replay
  sends days of history unpaced: the job runs as fast as its slowest stage, and every stage upstream
  of it reports backpressure. The stage that is *not* backpressured but busy near 100% is the
  bottleneck (`busy` in `infra.jsonl` and the Flink UI); on the 4-vCPU host that is `features:card`
  (17 features, state blobs written per event).
- **`bp high` during the measured window means the job cannot keep up with the offered rate**: the
  topic's consumer lag grows, the gateway reads staler features, and the report's freshness and
  sustainability checks say so. Scale the job (parallelism, more TaskManager cores) or reduce its cost.
- **Checkpoint duration** of 1–3 s with incremental sizes in the tens to hundreds of MB is normal
  during the replay. A duration approaching the timeout means barriers are stuck behind backlog or
  the checkpoint storage is slow; buffer debloating (`taskmanager.network.memory.buffer-debloat.enabled`)
  keeps about a second of data in flight so barriers stay fast.
- **Failed checkpoints**: open the job's exceptions in the Flink UI (or `health.flink_last_failure`
  in the run's manifest). "Coordinator is suspending" means the job itself was failing over: look for
  the first exception, not the checkpoint one.

## Configuration

Job arguments (`FraudJob`): `--mode`, `--entities`, `--parallelism`, `--max-parallelism`,
`--checkpoint-ms`, `--checkpoint-timeout-ms`, `--tolerable-checkpoint-failures`, `--exactly-once`,
`--buffer-timeout-ms`, `--group`, `--bootstrap`, `--events-topic`, `--decisions-topic`,
`--write-store`, `--store-state`, `--scoring-config`, `--async-capacity`, `--async-timeout-ms`.

Environment of the TaskManagers: `KAFKA_BOOTSTRAP`, `REDIS_URI`, `MODEL_TARGET`, `STATE_TTL_DAYS`,
`STORE_STATE`, `STORE_MAX_IN_FLIGHT`, `STORE_FAIL_AFTER_MS`, `STORE_FLUSH_TIMEOUT_MS`,
`REDIS_REQUEST_QUEUE`, `REDIS_COMMAND_TIMEOUT_MS`, `REDIS_CONNECT_TIMEOUT_MS`.

The runner chooses the parallelism (`FLINK_PARALLELISM`, default 2), the checkpoint interval
(`FLINK_CHECKPOINT_MS`) and the consumer group, and records the job's state provenance in Redis
(`realy:flink-job:<pipeline>`).

Cluster configuration: `x-flink-props` in `deploy/compose/docker-compose.yml` (one block for the
JobManager and the TaskManagers) and `flinkConfiguration` in `deploy/k8s/components/operators/flink.yaml`
/ `components/standalone/flink.yaml`.

## Changing the job

- **Add a feature**: edit the feature schema; no job code changes (the aggregator is driven by the DSL).
  Prove it with a verified run (`pulsebench run <scenario> --verify`): the Python twin
  (`python/realy/features/engine.py`) must agree exactly.
- **Add an operator**: give it a stable `.uid(...)` (state is matched by uid on restore). The runner
  restores with `allowNonRestoredState`, which on its own would silently drop the state of an operator
  whose uid disappeared; so the job record lists the job's stateful uids and, when the new graph lacks
  one, the runner starts fresh with the reason in the log instead (`runner/flink.py` `dropped_state`).
  Keep the source → features path unchained only where a `keyBy` requires it.
- **Test**: `FeatureJobTest` runs the keyed stages on a MiniCluster and checks them against sequential
  per-entity aggregation; `StateCodecTest` checks the state layout; `./gradlew :flink-job:test`.
