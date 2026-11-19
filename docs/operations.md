# Operations and troubleshooting

Most entries below come from a real failure found while building and running realy: the symptom,
the cause, and what the code or config now does about it.

## Health at a glance

* **Console, Environment page:** a card per component with health, version and latency, plus:
  * Kafka consumer-group lag (gateway, Flink, ClickHouse, collectors) and topic offsets;
  * Flink job state, checkpoints and backpressure;
  * Redis memory and keys;
  * ClickHouse rows and parts.
* **Same data from a shell:** `curl localhost:8090/api/env` (see the [HTTP API](reference/http-api.md)).
* **Docker:** `make ps`. Every service has a healthcheck, and the console starts only when the
  others are healthy.
* **Kubernetes:** `kubectl -n realy get pods`. Readiness probes gate the Services.

## The stack does not come up

| Symptom | Cause | Fix |
|---|---|---|
| `toomanyrequests` pulling base images | Docker Hub anonymous rate limit | mirror the base images ([deployment-docker.md](deployment-docker.md#registry-rate-limits-and-mirrors)) |
| Gradle `Could not GET ... 429` | Maven Central throttling | `MAVEN_MIRROR=...` |
| `PKIX path building failed`, `CERTIFICATE_VERIFY_FAILED`, `SELF_SIGNED_CERT_IN_CHAIN` during build | TLS-intercepting proxy | `REALY_BUILD_CA=<pem> make up-proxy` |
| ClickHouse container fails to start with `setrlimit ... operation not permitted` | the host forbids raising `nofile` (no `CAP_SYS_RESOURCE`) | leave the `ulimits` block commented out (the default) |
| ClickHouse `chown: Read-only file system` | format schemas mounted read-only where the entrypoint chowns | the init script copies them into the data volume (compose); an initContainer does it on Kubernetes |
| Console stuck in "waiting" on Kubernetes | `clickhouse-init` or `kafka-topics` Job still retrying | `kubectl -n realy logs job/clickhouse-init`; both retry until their dependency is up |
| Flink `Permission denied` on `/checkpoints` | named volume owned by root, Flink runs as uid 9999 | compose chowns before dropping privileges; Kubernetes uses `fsGroup: 9999` |

## Kubernetes (local overlays)

| Symptom | Cause | Fix |
|---|---|---|
| Pods stay `Pending` after `make k8s-local` | the node (Docker Desktop / kind VM / minikube) is smaller than the overlay's requests; `local-up.sh` prints a preflight warning | give it 6+ vCPU / 12+ GiB, or `REALY_KAFKA_BROKERS=1` (~9 GiB) |
| `k8s-test`: every *zero loss* / *verify* row PASS, *latency budget* rows FAIL | CPU-starved node: 3 replicating brokers plus the whole pipeline on 4 vCPUs (measured: 4-14 % idle, p99 200-300 ms) | more vCPUs, or 1 broker for latency work; correctness results stand regardless |
| A Kafka pod CrashLoops / no quorum | `kubectl -n realy logs realy-kafka-0` shows the `realy-kafka: node N/3, advertised ..., voters ...` line it derived; the voters must resolve via the headless Service | check `kubectl -n realy get endpoints realy-kafka-headless` (published before readiness) and that all 3 pods are scheduled |
| `local-up.sh` refuses: "the cluster runs N Kafka broker(s)" | switching between 1 and 3 brokers would reuse volumes formatted for another quorum | `make k8s-down`, then `make k8s-local` with the new `REALY_KAFKA_BROKERS` |
| Flink TaskManager `Pending` with a volume node-affinity / Multi-Attach event | the checkpoint volume is ReadWriteOnce; the TaskManager must share the JobManager's node (required pod affinity keeps it there) | single-node clusters are fine; on multi-node use object storage for `execution.checkpointing.dir` |
| Latency jumps for a few seconds mid-run, then recovers | a Kafka broker restarted (leader election, ISR shrink/expand) or the gateway rescaled | expected; the local overlays pin the gateway HPA to one replica so autoscaling never lands in a measurement |

## Runs that fail or look wrong

| Symptom | Cause | What realy does |
|---|---|---|
| Report flags a high **schedule lag** | the emitter host cannot keep up (CPU starved), so events leave late | open-loop pacing records intended vs sent time; late events count against the run ([architecture](architecture.md)) instead of silently lowering the load |
| First run after `make up` fails its p99, with seconds of latency in the first ~15 s | cold JVMs (JIT) and a just-submitted Flink job compete for CPU | the runner detects a cold pipeline (gateway scored < 20k events, or Flink job < 90 s old) and primes it with 30 s of unmeasured history events at the scenario's rate (`REALY_PRIME_S`; scored by the gateway or Flink, then discarded) before measuring (`cold pipeline ... priming` in the log; `auto_prime` in the manifest). Disable with `REALY_AUTO_PRIME=0`. Not applied to `flink_state: fresh` (exact verification) or the sync rail |
| False positives at the start of every run | velocity features begin empty (cold start), so "first card use" looks novel | `traffic.backfill_days` replays unpaced history first and excludes it from results |
| A run inherits a previous run's model delay or echo mode | a partial config left earlier fields set | every run pushes a **complete** `ScoringConfig` and `ModelServerConfig` (defaults filled in, [config](reference/config.md)) |
| ClickHouse row counts lower than decisions across runs | `ReplacingMergeTree` collapsed identical event ids from different runs | `run_id` is part of every sorting key; identity is `(run_id, event_id)` |
| Cross-process spans negative or skewed by about 10 ms | JVMs used an anchored monotonic clock that drifted from wall time | every service stamps `Clock.systemUTC()` / `time.time_ns()` ([architecture](architecture.md#clock-domains-and-in-band-telemetry)) |
| "Feature staleness" seems huge | it measured event-time age, not pipeline lag | the metric is `store_update_lag`: time from Kafka append to the online-store write |
| `JsonFormat: field already set` applying overrides | proto3 JSON oneof set twice by an override merge | overrides use a presence-preserving overlay before parsing |
| **FAIL** on `user-example-100ms` | intended: 100 ms feature build + 30-100 ms model cannot meet 150 ms | the budget check predicts it; the report shows fallbacks protecting availability |

## Throughput and tail latency

| Symptom | Cause | Fix in realy |
|---|---|---|
| Broker CPU high, p99 jitter with idle topics | consumers polling with `fetch.max.wait.ms=5` churn the broker | collectors and the gateway use 100 ms (latency is still bounded by `fetch.min.bytes=1`) |
| Collector burns a core while idle | busy-polling `consume(timeout=0)` | batch consume with a real timeout |
| About 20% of a core lost on the Kafka container | the healthcheck started a JVM (`kafka-topics.sh`) every 5 s | TCP probe healthcheck |
| Extra 1-3 ms on localhost hops | Docker userland proxy | `dockerd --userland-proxy=false` |
| Redis using CPU with no traffic | `io-threads` busy-poll | disabled on small hosts; worth it above about 100k ops/s on dedicated cores |
| TaskManager `OutOfMemoryError: Metaspace` after several job restarts | each job submission created a new user-code classloader | job jar in `lib/` with `classloader.resolve-order: parent-first` (one classloader per TaskManager) |
| Stop-with-savepoint fails and the old job keeps running | the savepoint failed mid-switch | the driver cancels the job, logs "state starts cold" and submits fresh ([runner/flink.py](../python/realy/runner/flink.py)) |
| p99 climbs with load past the knee | queueing in the model server (workers, batching) | step-ramp finds the knee; see [capacity-planning.md](capacity-planning.md) and the `capacity-saturation` scenario |

## Run history and gating

| Symptom | Cause | Fix |
|---|---|---|
| `run history: not stored in ClickHouse` in a run log | ClickHouse unreachable, or the `realy.runs` table missing (a stack created before it existed) | `docker compose ... exec clickhouse clickhouse-client --multiquery < contracts/storage/clickhouse/06_runs.sql`; history is best-effort and never fails a run (`REALY_RUN_HISTORY=0` disables it) |
| Grafana's `realy - run history` dashboard has no datasource | the ClickHouse plugin is downloaded from grafana.com at start (`GF_PLUGINS_PREINSTALL`); blocked egress prevents it | allow grafana.com, or bake the plugin into a Grafana image / mount it under `/var/lib/grafana/plugins` |
| `realy gate` fails `same scenario` | the baseline is from another scenario | gate against `baselines/<scenario>.json` (`realy gate RUN --save-baseline baselines/<scenario>.json` to adopt a run) |
| `realy size` warns "measured at only N events/s" | the calibration run was too light: fixed per-process costs dominate | calibrate from a step-ramp that reaches the design rate on the target nodes |

## Day-2 tasks

```bash
# Reset Flink state (next run starts cold) - or use flink_state: fresh in the scenario
docker compose -f deploy/compose/docker-compose.yml exec console realy run smoke --set flink_state=fresh

# Free disk: runs and worlds live in volumes
docker compose -f deploy/compose/docker-compose.yml exec console sh -c 'du -sh /runs/* | sort -h | tail'
docker compose -f deploy/compose/docker-compose.yml exec console rm -rf /runs/<run_id>

# ClickHouse audit queries
curl 'localhost:8123/?query=SELECT+run_id,count()+FROM+realy.decisions+GROUP+BY+run_id+FORMAT+PrettyCompact'

# Start over completely (deletes runs, worlds, checkpoints)
docker compose -f deploy/compose/docker-compose.yml down -v
```

On Kubernetes, run the same `realy` commands with `kubectl -n realy exec deploy/console -- ...`.
To reset Flink there, delete the `flink-checkpoints` PVC (standalone) or suspend and restore the
FlinkDeployment with `upgradeMode: stateless` (operators).
