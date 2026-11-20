#!/usr/bin/env bash
# realy on a local Kubernetes cluster, from source, in one command (make k8s-local). Kafka (3-broker
# KRaft) and Flink (session cluster) run inside the same cluster as the rest of the platform.
#   1. build the four realy images with Docker (same Dockerfile as the compose stack)
#   2. create a kind / k3d / minikube cluster, or use the current kubectl context
#   3. load the images into the cluster (no registry needed)
#   4. apply deploy/k8s/overlays/local (3 brokers) or overlays/local-1broker
#   5. wait for the Kafka quorum, topics, ClickHouse schema and every workload
# Then: make k8s-test (deploy/k8s/test.sh) proves it end to end.
# Console: http://localhost:${REALY_K8S_PORT:-8090}   Flink UI: http://localhost:${REALY_K8S_FLINK_PORT:-8081}
#
# Environment knobs (plus REALY_K8S / REALY_CLUSTER, see lib.sh):
#   REALY_KAFKA_BROKERS=3|1           3: RF 3 / min ISR 2 (default, ~11 GiB requested); 1: RF 1 (~9 GiB)
#   REALY_K8S_PORT=8090               host port for the console (kind/k3d port mapping to NodePort 30090)
#   REALY_K8S_FLINK_PORT=8081         host port for the Flink UI (NodePort 30081)
#   REALY_MINIKUBE_CPUS=6 REALY_MINIKUBE_MEMORY=12g   size of a new minikube profile
#   SKIP_BUILD=1                      reuse images already in the local Docker daemon
#   REALY_BUILD_CA=<pem> HTTPS_PROXY  build behind a TLS-intercepting proxy (adds the compose proxy override)
#   ./local-up.sh down                delete the kind/k3d cluster or minikube profile (current: the namespace)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
# shellcheck source=deploy/k8s/lib.sh
. "$ROOT/deploy/k8s/lib.sh"
PORT=${REALY_K8S_PORT:-8090}
FLINK_PORT=${REALY_K8S_FLINK_PORT:-8081}
BROKERS=${REALY_KAFKA_BROKERS:-3}
case "$BROKERS" in
  3) OVERLAY=local; NEED_CPU_M=3500; NEED_MEM_MI=11400 ;;
  1) OVERLAY=local-1broker; NEED_CPU_M=3000; NEED_MEM_MI=9300 ;;
  *) die "REALY_KAFKA_BROKERS must be 3 or 1" ;;
esac
IMAGES=(realy/scoring-gateway:dev realy/mock-model-server:dev realy/flink:dev realy/console:dev)

if [ "${1:-}" = down ]; then
  case "$PROVIDER" in
    kind) kind delete cluster --name "$CLUSTER" ;;
    k3d) k3d cluster delete "$CLUSTER" ;;
    minikube) minikube delete -p "$CLUSTER" ;;
    *) resolve_kubectl; k delete namespace "$NS" --ignore-not-found ;;
  esac
  exit 0
fi

# ---------------------------------------------------------------------------------------------- build
if [ -z "${SKIP_BUILD:-}" ]; then
  say "building images (gateway, mock-model, flink, console) from source"
  files=(-f "$ROOT/deploy/compose/docker-compose.yml")
  [ -n "${REALY_BUILD_CA:-}" ] && files+=(-f "$ROOT/deploy/compose/docker-compose.proxy.yml")
  docker compose "${files[@]}" build gateway mock-model flink-jobmanager console
fi
for img in "${IMAGES[@]}"; do docker image inspect "$img" >/dev/null 2>&1 || die "missing image $img (unset SKIP_BUILD)"; done

# ---------------------------------------------------------------------------------------------- cluster
case "$PROVIDER" in
  kind)
    if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
      say "creating kind cluster '$CLUSTER'"
      cat <<EOF | kind create cluster --name "$CLUSTER" --wait 300s --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - { containerPort: 30090, hostPort: ${PORT} }
      - { containerPort: 30081, hostPort: ${FLINK_PORT} }
EOF
    fi
    say "loading images into kind"
    kind load docker-image --name "$CLUSTER" "${IMAGES[@]}"
    ;;
  k3d)
    if ! k3d cluster get "$CLUSTER" >/dev/null 2>&1; then
      say "creating k3d cluster '$CLUSTER'"
      k3d cluster create "$CLUSTER" --wait --timeout 300s \
        -p "${PORT}:30090@server:0" -p "${FLINK_PORT}:30081@server:0" \
        --k3s-arg "--disable=traefik@server:0" --k3s-arg "--disable=metrics-server@server:0"
    fi
    say "importing images into k3d"
    k3d image import -c "$CLUSTER" "${IMAGES[@]}"
    ;;
  minikube)
    if ! minikube status -p "$CLUSTER" >/dev/null 2>&1; then
      say "starting minikube profile '$CLUSTER'"
      minikube start -p "$CLUSTER" --cpus "${REALY_MINIKUBE_CPUS:-6}" --memory "${REALY_MINIKUBE_MEMORY:-12g}"
    fi
    say "loading images into minikube"
    for img in "${IMAGES[@]}"; do minikube -p "$CLUSTER" image load "$img"; done
    ;;
  current)
    say "using kubectl context '$(kubectl config current-context)': its nodes must be able to use the local"
    say "realy/* images (Docker Desktop's built-in cluster can; otherwise push them to a registry)"
    ;;
esac
resolve_kubectl
[ "$HOST_KUBECTL" = 1 ] || say "kubectl not found on the host - using the one inside $NODE"

# ---------------------------------------------------------------------------------------------- preflight
# Requested resources of the overlay vs what the node offers (scheduling fails silently otherwise:
# pods just stay Pending).
alloc=$(k get nodes -o jsonpath='{range .items[*]}{.status.allocatable.cpu} {.status.allocatable.memory}{"\n"}{end}' 2>/dev/null | head -1)
if [ -n "$alloc" ]; then
  cpu=${alloc%% *}; mem=${alloc##* }
  case "$cpu" in *m) cpu_m=${cpu%m} ;; *) cpu_m=$((cpu * 1000)) ;; esac
  case "$mem" in *Ki) mem_mi=$((${mem%Ki} / 1024)) ;; *Mi) mem_mi=${mem%Mi} ;; *Gi) mem_mi=$((${mem%Gi} * 1024)) ;; *) mem_mi=$((mem / 1048576)) ;; esac
  say "node allocatable: $((cpu_m / 1000)) CPU / $((mem_mi / 1024)) GiB - overlays/$OVERLAY requests ~$((NEED_CPU_M / 1000)).$(( (NEED_CPU_M % 1000) / 100 )) CPU / $((NEED_MEM_MI / 1024)) GiB"
  if [ "$cpu_m" -lt "$NEED_CPU_M" ] || [ "$mem_mi" -lt "$NEED_MEM_MI" ]; then
    warn "the node is smaller than overlays/$OVERLAY requests: some pods will stay Pending."
    warn "Give Docker / the VM more CPU and memory, or use REALY_KAFKA_BROKERS=1."
  fi
fi

# A running Kafka with a different broker count has volumes formatted for another quorum, and Flink
# checkpoints hold offsets of topics that would be re-created: switching needs a clean slate.
cur=$(k -n "$NS" get statefulset realy-kafka -o jsonpath='{.spec.replicas}' 2>/dev/null || true)
if [ -n "$cur" ] && [ "$cur" != "$BROKERS" ]; then
  die "the cluster runs $cur Kafka broker(s); switching to $BROKERS needs a fresh deployment: run '$0 down' first (or keep REALY_KAFKA_BROKERS=$cur)"
fi

# ---------------------------------------------------------------------------------------------- deploy
say "applying deploy/k8s/overlays/$OVERLAY"
# Jobs are immutable: drop finished init Jobs so a re-apply re-runs them against the current manifests
k -n "$NS" delete job kafka-topics clickhouse-init --ignore-not-found >/dev/null 2>&1 || true
if [ "$HOST_KUBECTL" = 1 ]; then
  k apply -k "$ROOT/deploy/k8s/overlays/$OVERLAY"
else
  tar -C "$ROOT/deploy/k8s" -cf - . | docker exec -i "$NODE" sh -c \
    "rm -rf /tmp/realy-k8s && mkdir -p /tmp/realy-k8s && tar -C /tmp/realy-k8s -xf - && \
     ${NODE_KUBECTL[*]:4} apply -k /tmp/realy-k8s/overlays/$OVERLAY"
fi

say "waiting for the Kafka quorum ($BROKERS broker(s)), topics and the ClickHouse schema"
k -n "$NS" rollout status statefulset/realy-kafka --timeout=600s
k -n "$NS" wait --for=condition=complete job/kafka-topics --timeout=600s
k -n "$NS" rollout status statefulset/clickhouse --timeout=600s
k -n "$NS" wait --for=condition=complete job/clickhouse-init --timeout=600s
say "waiting for the services"
for w in statefulset/redis deployment/mock-model deployment/gateway deployment/flink-jobmanager \
         deployment/flink-taskmanager deployment/console; do
  k -n "$NS" rollout status "$w" --timeout=600s
done
k -n "$NS" get pods -o wide

url="http://localhost:${PORT}"; furl="http://localhost:${FLINK_PORT}"
case "$PROVIDER" in minikube|current) url="(kubectl -n $NS port-forward svc/console ${PORT}:8090, then http://localhost:${PORT})"
                                      furl="(kubectl -n $NS port-forward svc/flink-jobmanager ${FLINK_PORT}:8081)" ;; esac
cat <<EOF

realy is up on Kubernetes ($PROVIDER, $BROKERS Kafka broker(s), Flink session cluster).
  console   $url
  Flink UI  $furl
  test      make k8s-test          (smoke, flink-inline + verify, broker failover)
  CLI       ${KUBECTL[*]} -n $NS exec -it deploy/console -- realy run smoke
  tear down $0 down
EOF
exit 0
