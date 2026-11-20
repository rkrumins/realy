# Shared by deploy/k8s/local-up.sh and deploy/k8s/test.sh (sourced, not executed).
# Resolves the cluster provider and a kubectl command for it:
#   REALY_K8S=auto|kind|k3d|minikube|current   auto: kind, k3d, minikube (first installed), else the
#                                               current kubectl context. k3d pulls helper images from
#                                               ghcr.io; kind's node image comes from Docker Hub.
#   REALY_CLUSTER=realy                         cluster / profile name
# Without kubectl on the host, the kubectl inside the kind / k3d node container is used.
# shellcheck shell=bash disable=SC2034  # variables are consumed by the sourcing scripts
NS=${REALY_NAMESPACE:-realy}
PROVIDER=${REALY_K8S:-auto}
CLUSTER=${REALY_CLUSTER:-realy}

say() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33mwarning: %s\033[0m\n' "$*" >&2; }
die() { printf '\033[1;31merror: %s\033[0m\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

if [ "$PROVIDER" = auto ]; then
  if have kind; then PROVIDER=kind
  elif have k3d; then PROVIDER=k3d
  elif have minikube; then PROVIDER=minikube
  elif have kubectl && kubectl config current-context >/dev/null 2>&1; then PROVIDER=current
  else die "need kind (https://kind.sigs.k8s.io), k3d, minikube, or kubectl with a current context"; fi
fi

case "$PROVIDER" in
  kind) CONTEXT="kind-${CLUSTER}"; NODE="${CLUSTER}-control-plane"
        NODE_KUBECTL=(docker exec -i "$NODE" kubectl --kubeconfig /etc/kubernetes/admin.conf) ;;
  k3d) CONTEXT="k3d-${CLUSTER}"; NODE="k3d-${CLUSTER}-server-0"; NODE_KUBECTL=(docker exec -i "$NODE" kubectl) ;;
  minikube) CONTEXT="${CLUSTER}"; NODE=""; NODE_KUBECTL=() ;;
  current) CONTEXT=""; NODE=""; NODE_KUBECTL=() ;;
  *) die "unknown REALY_K8S=$PROVIDER (kind | k3d | minikube | current)" ;;
esac

# call after the cluster exists
resolve_kubectl() {
  if have kubectl; then
    if [ -n "$CONTEXT" ]; then KUBECTL=(kubectl --context "$CONTEXT"); else KUBECTL=(kubectl); fi
    HOST_KUBECTL=1
  elif [ "${#NODE_KUBECTL[@]}" -gt 0 ]; then
    KUBECTL=("${NODE_KUBECTL[@]}")
    HOST_KUBECTL=0
  else
    die "kubectl not found"
  fi
}
k() { "${KUBECTL[@]}" "$@"; }
