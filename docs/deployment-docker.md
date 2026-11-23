# Deployment with Docker

One multi-target Dockerfile (`deploy/docker/Dockerfile`, build context = repository root) builds every
image from source. One compose file (`deploy/compose/docker-compose.yml`) runs the whole platform.

```bash
make up          # build + start          make down     stop (volumes kept)
make ps          # health of every service make logs     follow logs
make images      # build only              make up-obs   + Prometheus / Grafana
```

## Images

| Target | Image | Base | Contents |
|---|---|---|---|
| `java-build` | (stage) | `gradle:8.14-jdk21` | `gradle installDist :flink-job:shadowJar` with a cached `~/.gradle` |
| `ui-build` | (stage) | `node:22-alpine` | `npm ci && npm run build`, the Tailwind console |
| `gateway` | `realy/scoring-gateway:dev` | `eclipse-temurin:21-jre` | scoring gateway distribution, ZGC |
| `mock-model` | `realy/mock-model-server:dev` | `eclipse-temurin:21-jre` | KServe v2 mock and `models/proxy` weights |
| `flink` | `realy/flink:dev` | `flink:2.2.1-java21` | job jar at `/opt/flink/lib/realy/` on the system classpath |
| `console` | `realy/console:dev` | `python:3.13-slim` + JRE 21 | pulsebench Python package, UI build, Java load emitter, Flink job jar, scenarios, docs |

The same four images run on Kubernetes (see [deployment-kubernetes.md](deployment-kubernetes.md)).
In the flink image the job jar loads **parent-first** from `lib/`, so a TaskManager holds one
classloader for its whole life. Job restarts and topology switches therefore never leak Metaspace
(see [operations.md](operations.md)).

## Configuration

Compose reads these from the environment or an `.env` file next to the compose file:

| Variable | Default | Effect |
|---|---|---|
| `REALY_CONSOLE_TOKEN` | *(empty)* | enables token login on the console ([security.md](security.md)) |
| `MODEL_WEIGHTS` | `/models/proxy/fraud_mlp.weights.json` | proxy model the mock serves; empty = bundled default weights |
| `GRADLE_IMAGE`, `NODE_IMAGE`, `PYTHON_IMAGE`, `JRE_IMAGE`, `FLINK_IMAGE` | Docker Hub images | base-image overrides (registry mirrors) |
| `MAVEN_MIRROR` | *(empty)* | Maven repository tried before Maven Central (Nexus, Artifactory, a public mirror) |
| `FLINK_UI_URL`, `GRAFANA_URL`, `PROMETHEUS_URL` | `http://localhost:...` | links shown on the console's Environment page |

The console container keeps its state in two volumes:
* `realy-runs` at `/runs`: run directories with Parquet, reports and logs;
* `realy-data` at `/data`: generated worlds (`/data/world-<customers>-<seed>`) and scenarios saved
  from the builder (`/data/scenarios`).

Other services keep theirs:
* `clickhouse-data` at `/var/lib/clickhouse`: the audit and analytics tables. The secured overlay uses
  `clickhouse-secured` instead, because the Kafka engine tables carry their profile's broker list.
* `gateway-spool` at `/spool`: the decision WAL spool.
* `flink-checkpoints` at `/checkpoints`: retained checkpoints and savepoints.
* `realy-models` at `/models`: the served model versions.

On start the bundled scenario packs are copied into `/data/scenarios`. Copies you have not edited
follow the image when it is updated. Edited ones are kept, and flagged in the console when the bundled
version changed ("Reset" restores it). The paths can be overridden with `REALY_RUNS`, `REALY_DATA`, `REALY_SCENARIOS`
and `REALY_HOME`.

## Building behind a corporate / TLS-intercepting proxy

If your network re-signs TLS with its own CA, builds fail with certificate errors from apt, pip, npm
or Gradle. Pass the CA as a BuildKit **secret** and the proxy as a build arg:

```bash
REALY_BUILD_CA=/path/to/corp-ca.pem HTTPS_PROXY=http://proxy.corp:3128 make up-proxy
```

`deploy/compose/docker-compose.proxy.yml` adds three things to every build:
* `network: host`, so build steps can reach a proxy on the host's loopback;
* the `HTTPS_PROXY` build arg;
* the `ca` secret.

`deploy/docker/build-trust.sh` installs the CA (a PEM file, which may hold several certificates) for
the OS, Java (`keytool`), pip and npm, and gives Gradle the proxy settings. This happens during the
build only. A secret is never written to an image layer, so the images you ship contain neither
the CA nor the proxy.

## Registry rate limits and mirrors

* **Docker Hub anonymous pull limits:** point the base images at a mirror:
  `PYTHON_IMAGE=mirror.gcr.io/library/python:3.13-slim JRE_IMAGE=mirror.gcr.io/library/eclipse-temurin:21-jre make up`.
  For `kafka`, `redis` and `clickhouse`, which are pulled rather than built, use
  `docker pull mirror.gcr.io/library/<image>` and `docker tag` it as the name in the compose file.
* **Maven Central HTTP 429 or a blocked Maven Central:**
  `MAVEN_MIRROR=https://maven-central.storage-download.googleapis.com/maven2 make up`, or your
  Nexus/Artifactory URL. Gradle tries the mirror first, then the Gradle plugin portal and Maven Central.

## Host networking and performance notes

* Start dockerd with `--userland-proxy=false` on Linux benchmark hosts. The userland proxy copies
  every published-port packet through a process and adds tail latency on `localhost:9092`.
* The console container has `shm_size: 1g` for the emitter's shared-memory batches.
* ClickHouse wants a high `nofile` limit. On hosts that allow it, uncomment the `ulimits` block on
  the `clickhouse` service. Some sandboxes lack `CAP_SYS_RESOURCE` and refuse it.
* The compose stack is sized for a 4 vCPU / 16 GB machine (about 1-2k TPS sustained with every
  component on one host). The 10k TPS target needs the Kubernetes layout in
  [k8s-l4.md](k8s-l4.md); see [capacity-planning.md](capacity-planning.md).

## Developing against the stack

Rebuild and restart a single service after a code change:

```bash
docker compose -f deploy/compose/docker-compose.yml up -d --build gateway
make flink-reload                 # Flink image -> JobManager + TaskManager
```

To work on the UI or the Python code on the host against the running stack, see
[developer-guide.md](developer-guide.md) (`make dev-ui`, `make dev-console`).
