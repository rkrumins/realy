# Running against real infrastructure

PulseBench runs the same pipeline on the bundled stack (Docker Compose, the local Kubernetes overlays) or
on infrastructure you run: your Kafka, Redis, model server, Flink and ClickHouse. Every component is
reached through settings, so moving from the bundled stack to yours is configuration. This page lists
those settings component by component, explains what the preflight checks before a run, and says what
changes in the results.

## Three ways to run

| Way | What is yours | How |
|---|---|---|
| **The bundled stack** | nothing: everything runs in Docker or the local cluster | `make up`; `pulsebench run smoke` |
| **The pipeline on your infrastructure** | Kafka, Redis, model server, ClickHouse (and optionally Flink) | Compose: `docker-compose.external.yml` with `external.env`. Kubernetes: the same environment variables on the gateway, the Flink TaskManagers and the console (the production overlay shows every one) |
| **Your own scorer** | the decision service too: PulseBench only drives load and measures | planned (a `scorer: external` topology): your service consumes the events topic and publishes `DecisionRecord`s with the trace it received |

In every case the benchmark (the emitter, collector, runner, verifier and console) is the instrument, and
the gateway, the Flink job, the model server and the stores are the system under test.

## Before a run: the preflight

```
pulsebench infra check flows-smoke          # in the console container, or anywhere with the settings
```

It probes every component the scenario's topology uses, through the same settings the services read, and
says for each one whether it is **bundled**, a **mock** or **external** (yours). Each check is pass, warn
or fail, with the fix:

```
preflight (gateway-kafka): WARN
  Kafka            external  3 broker(s), SASL_SSL        b-1.msk.example.com:9096
    ok   reachable    3 broker(s) over SASL_SSL
    ok   topics       all 6 contract topics present
    ok   partitions   payments.events.raw: 48 partitions
  Redis            external  7.1.0 cluster                rediss://clustercfg.fraud.example.com:6379
    ok   reachable    cluster, ping p50 0.41 ms / p99 1.20 ms
    WARN eviction     maxmemory-policy allkeys-lru can evict profiles and state blobs silently
  Model server     external  onnxruntime_onnx 3           fraud-model.example.com:443
    ok   metadata     fraud_mlp on onnxruntime_onnx, input input__0 [62] -> output__0
  ...
```

Every run does the same as its first startup step. It records the result in the run (`infra.json`, the
manifest's `infra`), logs every warning, and stops on a failed check, which means the run would measure or
verify the wrong thing. `REALY_PREFLIGHT=warn` runs anyway; `off` skips the check.

The gateway reports its own view (`GET /admin/info`), because its network and settings are the ones that
matter. That view covers:
- the pipeline and feature-schema hash it runs;
- the Kafka it consumes;
- whether it reaches the store as standalone or cluster, and the server's version;
- the served model's platform, version and input width;
- the audit sink;
- every simulated setting in effect.

The preflight fails when the gateway runs a different pipeline or schema from the runner.

## Real and simulated, in the results

A run on the bundled stack measures real Kafka, Redis, Flink and ClickHouse on one host. The model
server is the mock. A run can also add latency on purpose. The run's record says which parts were
simulated, so a result is never read as a measurement of something it was not:

| Simulated part | When | Recorded as |
|---|---|---|
| Model latency | the mock model server (its metadata reports platform `realy_mock`): inference time is drawn from `model_server.service_time`, not computed | `simulated: model latency` |
| Feature-lookup latency | `scoring.feature_lookup_delay` | `simulated: feature-lookup latency` |
| Model-call latency | `scoring.model_extra_delay` | `simulated: model-call latency` |
| Features | `scoring.feature_source: mock` | `simulated: features` |
| Decisions | the `echo-kafka` topology | `simulated: decisions` |

## Component by component

### Kafka

| Setting | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` (and `FLINK_KAFKA_BOOTSTRAP` for the job the runner submits) | `localhost:9092` | your brokers |
| `KAFKA_SECURITY_PROTOCOL` | PLAINTEXT | `SASL_SSL`, `SSL`, `SASL_PLAINTEXT` |
| `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD_FILE` (or `_PASSWORD`) | | SCRAM / PLAIN credentials; `KAFKA_SASL_JAAS_CONFIG` for anything else (OAUTHBEARER) |
| `KAFKA_SSL_*` | | a private CA, mutual TLS (Java: trust/key stores; Python: PEM locations) |
| `REALY_KAFKA_TOPICS` | manage | `verify` on a managed cluster: PulseBench creates nothing, checks that every topic in `contracts/kafka/topics.yaml` exists with enough partitions, and names what to create |
| topic names | `contracts/pipelines/payments.yaml` `topics` | events, decisions, labels, assembled, dlq |

Production topics need replication 3 with `min.insync.replicas` 2, so they survive a broker. The events
topic needs at least as many partitions as consumers sharing it: gateway replicas × `KAFKA_CONSUMERS`,
or Flink source tasks. The preflight warns otherwise.

### Redis

| Setting | Default | Meaning |
|---|---|---|
| `REDIS_URI` | `redis://localhost:6379` | `rediss://` for TLS, `user:password@` for ACL auth |
| `REDIS_MODE` | auto | `auto` asks the server (`INFO cluster`); `cluster` for Redis Cluster (ElastiCache cluster mode, MemoryDB, Memorystore Cluster, Redis Enterprise's OSS cluster API); `standalone` |
| `REDIS_CA_FILE` | | a private CA (Python callers; Java uses the JVM trust store) |
| `REDIS_TOPOLOGY_REFRESH_S` | 30 | cluster topology refresh, plus refresh on MOVED / ASK and reconnects |

The online store holds one blob per entity and the profiles; nothing else. Use `noeviction` (sized with
headroom) or a `volatile-*` policy: `allkeys-*` evicts profiles silently, and the preflight warns about
it. Put the store in the scorer's zone: every event pays one round trip to it (the preflight measures the
ping from where it runs).

### Model server

Any KServe v2 gRPC server works: Triton, KServe, Seldon MLServer. The gateway and the Flink inline job
call it with the feature vector in schema order ([feature-store-and-inference.md](feature-store-and-inference.md)).

| Setting | Default | Meaning |
|---|---|---|
| `MODEL_TARGET`, `MODEL_NAME` | `localhost:8001`, `fraud_mlp` | the endpoint and the model |
| `MODEL_INPUT_NAME`, `MODEL_OUTPUT_NAME` | `input__0`, `output__0` | the tensors |
| `MODEL_INPUT_DIM` | the model's metadata | the input width, when the metadata cannot be read (required then in the production profile) |
| `MODEL_TLS`, `MODEL_CA_FILE`, `MODEL_CLIENT_CERT` / `_KEY` | off | TLS, a private CA, mutual TLS |
| `MODEL_AUTH_HEADER`, `MODEL_AUTH_TOKEN_FILE` (or `_TOKEN`) | | a header on every call (`authorization: Bearer ...`) |
| `MODEL_AUTHORITY` | | the `:authority` to send when it differs from the target (KServe behind an ingress gateway) |

A score that is missing, NaN or outside [0, 1] is a model error, not a silent 0. The orchestrator then
applies its deadline policy (rules-only by default).

The `model_server.*` scenario settings only apply to the mock server. Against a real server, the run's
record says the model is external. The verifier recomputes the score only for the bundled proxy model:
for another model, score parity is reported as not checkable, with the reason.

### Flink

| Setting | Default | Meaning |
|---|---|---|
| `FLINK_REST` | `http://localhost:8081` | the cluster the runner submits the job to (session cluster), or that it observes |
| `FLINK_PARALLELISM`, `MAX_PARALLELISM` | 2, 128 | the job's parallelism; key groups (the rescaling ceiling) |
| `FLINK_CHECKPOINTS_URI` | `file:///checkpoints` | durable shared storage in production (`s3://`, `gs://`, `hdfs://`); the preflight warns about a local path |
| `REDIS_URI`, `KAFKA_*`, `MODEL_*` on the TaskManagers | | the job reaches the same infrastructure as the gateway |

With the Flink Kubernetes Operator, the operator runs the job (`deploy/k8s/overlays/production/flink.yaml`)
and the runner only observes it.

### ClickHouse

| Setting | Default | Meaning |
|---|---|---|
| `CLICKHOUSE_URL` | `http://localhost:8123` | `https://` for TLS (ClickHouse Cloud: port 8443) |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD_FILE` (or `_PASSWORD`) | | sent in ClickHouse's own headers, never in the URL |
| `CLICKHOUSE_CA_FILE` | | a private CA |

The schema is `contracts/storage/clickhouse/*.sql`, applied with `make migrate` or by your own migration
tooling. The Kafka engine tables read the decisions, labels and events topics with ClickHouse's own Kafka
client. Against a SASL / TLS cluster, give ClickHouse the settings in its server config:

```xml
<clickhouse>
  <kafka>
    <security_protocol>SASL_SSL</security_protocol>
    <sasl_mechanism>SCRAM-SHA-512</sasl_mechanism>
    <sasl_username>pulsebench</sasl_username>
    <sasl_password from_env="KAFKA_SASL_PASSWORD"/>
  </kafka>
</clickhouse>
```

The init scripts replace `__KAFKA_BROKERS__` in the DDL with your broker list.

### The gateway's admin API

`PUT /admin/config` changes scoring live (the benchmark uses it to apply a scenario). Set `ADMIN_TOKEN_FILE`
(or `ADMIN_TOKEN`) on the gateway and the mock model server, and `REALY_ADMIN_TOKEN` on the runner, which
then sends `Authorization: Bearer <token>`. `ADMIN_WRITE=false` refuses writes altogether.

## What verification proves against real infrastructure

| Check | Bundled stack | Your infrastructure |
|---|---|---|
| Every decision collected, none lost, duplicates counted | yes | yes |
| End-to-end latency on one clock | yes | yes: the emitter and collector share the driver's clock wherever the pipeline runs |
| Customer and card features against the Python reference | exact-set target per topology | the same; store lag on a slower network shows as a lower agreement, with the reason |
| Rules and outcome parity | 100% | 100% (the rules are CEL, recomputed from the logged inputs) |
| Score parity | the bundled proxy model | only with the proxy's weights; otherwise reported as not checkable |
| The audit in ClickHouse is complete | yes | yes, with `CLICKHOUSE_*` set |

## Secrets

Secrets are never in scenarios, contracts or logs:
- **Files:** every credential setting has a `_FILE` form for a mounted secret (Kafka SASL, model token,
  ClickHouse password, admin token).
- **Endpoints:** reported (by `/admin/info` and the preflight) without user info.
- **Headers:** ClickHouse credentials travel in headers, not URLs.
