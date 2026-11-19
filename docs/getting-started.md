# Getting started

All you need is **Docker** (Engine 24+ with Compose v2, or Docker Desktop) and, for the stack,
4 CPUs and about 10 GB of free RAM (Docker Desktop: raise its memory limit). Everything (Java services, Flink job, UI, Python driver) is built from source
inside Docker, so no JDK, Node or Python is needed on the host.

```bash
git clone <this repository> realy && cd realy
make up                      # = docker compose -f deploy/compose/docker-compose.yml up -d --build
open http://localhost:8090   # the operator console
```

The first `make up` downloads base images and dependencies and compiles everything: about 5-10
minutes. Later builds are cached and take seconds. When `docker compose ps` shows every service
healthy, the console is ready.

## Your first run

1. Open **Launch**, pick **smoke** (500 TPS for 60 s through the async gateway) and press **Start**.
   The inline budget check first estimates whether the scenario can meet its p99 budget.
2. The console switches to **Live**. It shows:
   * the topology map, with events/s and p99 on every hop;
   * emitted vs decided throughput;
   * p50/p99 against the budget band;
   * per-stage latency;
   * a feed of real decisions as they happen.
3. When the run drains, open its **report**:
   * the verdict and the reasons for it;
   * the latency waterfall per step;
   * the funnel (emitted → decided → audited);
   * detection quality per fraud typology.
4. Open **Explorer** and click any decision. You get every input feature, the rules that fired,
   the model score, the ground-truth label and a span waterfall of where the time went.

The same run from a terminal:

```bash
make smoke                                          # realy run smoke, inside the console container
make console-shell                                  # a shell with the realy CLI
realy scenarios                                     #   list the scenario packs
realy budget step-ramp                              #   Monte Carlo feasibility before a long run
realy run flink-inline-smoke && realy verify $(ls -td /runs/*/ | head -1)
```

## What is running

| Service | Port | What it is |
|---|---|---|
| console | 8090 | UI + API + bench driver (emitters and collectors) |
| kafka | 9092 | KRaft single broker, topics from `contracts/kafka/topics.yaml` |
| flink-jobmanager / taskmanager | 8081 | Flink 2.2 session cluster with the realy job on RocksDB |
| redis | 6379 | online feature store |
| gateway | 8080 | scoring gateway (gRPC + HTTP admin + Kafka ingress) |
| mock-model | 8001 / 8002 | KServe v2 gRPC mock model with a capacity model / admin |
| clickhouse | 8123 / 9000 | async audit and analytics store, fed by its Kafka engine |
| prometheus / grafana | 9090 / 3000 | optional: `make up-obs` |

Stop with `make down`. Runs, generated worlds and Flink checkpoints are kept in Docker volumes
until `docker compose -f deploy/compose/docker-compose.yml down -v`.

## Next steps

* [Console guide](console-guide.md): every page of the UI.
* [Scenarios](scenarios.md): write your own load and fraud scenarios, visually or as YAML.
* [Running benchmarks](running-benchmarks.md): what a run does and how to read a report.
* [Deployment with Docker](deployment-docker.md): building behind proxies, mirrors, tuning.
* [Kubernetes](deployment-kubernetes.md): a local cluster in one command, then GKE with L4 GPUs.
* [Architecture](architecture.md): how the measurement works and why you can trust it.
