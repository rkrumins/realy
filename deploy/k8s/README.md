# PulseBench on Kubernetes

Kustomize base, components and overlays for the whole platform on a cluster. The full guide (sizing,
GKE + L4, operators, what each overlay changes) is [docs/deployment-kubernetes.md](../../docs/deployment-kubernetes.md).

| Path | What it is |
|---|---|
| `base/` | namespace, Redis, ClickHouse, gateway (Deployment + HPA + PDB), mock model server, console; `generated/` holds the ClickHouse DDL and config maps written by `make codegen` |
| `components/operators/` | Strimzi Kafka and the Flink Kubernetes operator (JobManager HA), for clusters that run operators |
| `components/standalone/` | Kafka (KRaft) and a Flink session cluster as plain workloads, for clusters without operators |
| `components/kafka-3-brokers/` | three brokers, RF 3, min ISR 2 |
| `components/local-sizing/` | requests and limits that fit a laptop |
| `components/bench-job/` | a run as a Kubernetes Job (`pulsebench run <scenario>`) |
| `components/console-ingress/` | an Ingress for the console |
| `overlays/local`, `overlays/local-1broker` | kind / k3d / minikube from source: `make k8s-local`, then `make k8s-test` |
| `overlays/dev` | a shared dev cluster |
| `overlays/gke-l4` | GKE with L4 GPUs and Triton behind the KServe v2 contract |

Scripts: `local-up.sh` (build images, create or reuse a cluster, load images, apply, wait), `test.sh`
(end-to-end acceptance against the cluster), `lib.sh` (shared helpers and the `REALY_K8S` /
`REALY_CLUSTER` knobs).

Checks: `make k8s-validate` renders every overlay; `python -m realy.k8s` regenerates `base/generated`
(the staleness test fails when it is out of date).

State that must survive restarts: Flink checkpoints and savepoints (a PVC in the Flink components),
Redis (optional persistence, `REDIS_PERSIST`), ClickHouse data (PVC), the runs volume (console). The
gateway's audit spool is an `emptyDir` (it survives container restarts, not pod deletion; use a
StatefulSet with a PVC where it must outlive the pod).
