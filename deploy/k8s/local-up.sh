#!/usr/bin/env bash
# realy on a local Kubernetes cluster, from source, in one command (make k8s-local):
#   1. build the four realy images with Docker (same Dockerfile as the compose stack)
#   2. create a k3d (or kind) cluster - or reuse the current kubectl context
#   3. import the images into the cluster (no registry needed)
#   4. apply deploy/k8s/overlays/local (no operators, no CRDs)
#   5. wait for Kafka topics, the ClickHouse schema and every workload to be ready
# Console: http://localhost:${REALY_K8S_PORT:-8090}   Flink UI: http://localhost:${REALY_K8S_FLINK_PORT:-8081}
#
# Environment knobs:
#   REALY_K8S=auto|k3d|kind|current   cluster provider (auto: k3d, then kind, then current context)
#   REALY_CLUSTER=realy               cluster name for k3d/kind
#   REALY_K8S_PORT=8090               host port for the console (k3d/kind port mapping to NodePort 30090)
#   REALY_K8S_FLINK_PORT=8081         host port for the Flink UI (NodePort 30081)
#   SKIP_BUILD=1                      reuse images already in the local Docker daemon
#   REALY_BUILD_CA=<pem> HTTPS_PROXY  build behind a TLS-intercepting proxy (adds the compose proxy override)
#   ./local-up.sh down                delete the k3d/kind cluster
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROVIDER=${REALY_K8S:-auto}
CLUSTER=${REALY_CLUSTER:-realy}
PORT=${REALY_K8S_PORT:-8090}
FLINK_PORT=${REALY_K8S_FLINK_PORT:-8081}
NS=realy
IMAGES=(realy/scoring-gateway:dev realy/mock-model-server:dev realy/flink:dev realy/console:dev)

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31merror: %s\033[0m\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

if [ "$PROVIDER" = auto ]; then
  if have k3d; then PROVIDER=k3d; elif have kind; then PROVIDER=kind
  elif have kubectl && kubectl config current-context >/dev/null 2>&1; then PROVIDER=current
  else die "need k3d (https://k3d.io), kind, or kubectl with a current context"; fi
fi

if [ "${1:-}" = down ]; then
  case "$PROVIDER" in
    k3d) k3d cluster delete "$CLUSTER" ;;
    kind) kind delete cluster --name "$CLUSTER" ;;
    *) kubectl delete namespace "$NS" --ignore-not-found ;;
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
  k3d)
    if ! k3d cluster get "$CLUSTER" >/dev/null 2>&1; then
      say "creating k3d cluster '$CLUSTER'"
      k3d cluster create "$CLUSTER" --wait --timeout 300s \
        -p "${PORT}:30090@server:0" -p "${FLINK_PORT}:30081@server:0" \
        --k3s-arg "--disable=traefik@server:0" --k3s-arg "--disable=metrics-server@server:0"
    fi
    say "importing images into k3d"
    k3d image import -c "$CLUSTER" "${IMAGES[@]}"
    SERVER="k3d-${CLUSTER}-server-0"
    ;;
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
    SERVER="${CLUSTER}-control-plane"
    ;;
  current)
    say "using kubectl context '$(kubectl config current-context)' - images must be pullable by its nodes"
    SERVER=""
    ;;
  *) die "unknown REALY_K8S=$PROVIDER" ;;
esac

# kubectl on the host if present, otherwise the one inside the k3s/kind node container
if have kubectl; then
  if [ "$PROVIDER" = k3d ]; then KUBECTL=(kubectl --context "k3d-${CLUSTER}")
  elif [ "$PROVIDER" = kind ]; then KUBECTL=(kubectl --context "kind-${CLUSTER}")
  else KUBECTL=(kubectl); fi
  apply_overlay() { "${KUBECTL[@]}" apply -k "$ROOT/deploy/k8s/overlays/local"; }
else
  [ -n "$SERVER" ] || die "kubectl not found"
  say "kubectl not found on the host - using the one inside $SERVER"
  KUBECTL=(docker exec -i "$SERVER" kubectl)
  apply_overlay() {
    tar -C "$ROOT/deploy/k8s" -cf - . | docker exec -i "$SERVER" sh -c \
      'rm -rf /tmp/realy-k8s && mkdir -p /tmp/realy-k8s && tar -C /tmp/realy-k8s -xf - && kubectl apply -k /tmp/realy-k8s/overlays/local'
  }
fi
k() { "${KUBECTL[@]}" "$@"; }

# ---------------------------------------------------------------------------------------------- deploy
say "applying deploy/k8s/overlays/local"
# Jobs are immutable: drop finished init Jobs so a re-apply re-runs them against the current manifests
k -n "$NS" delete job kafka-topics clickhouse-init --ignore-not-found >/dev/null 2>&1 || true
apply_overlay

say "waiting for Kafka, topics and the ClickHouse schema"
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

cat <<EOF

realy is up on Kubernetes ($PROVIDER).
  console   http://localhost:${PORT}
  Flink UI  http://localhost:${FLINK_PORT}
  CLI       ${KUBECTL[*]} -n $NS exec -it deploy/console -- realy run smoke
  tear down $0 down
EOF
[ "$PROVIDER" = current ] && echo "  (context 'current': kubectl -n $NS port-forward svc/console ${PORT}:8090)"
exit 0
