#!/usr/bin/env bash
# Start one broker of the standalone KRaft Kafka StatefulSet (each pod is broker + controller).
# Derives the per-pod identity from the StatefulSet ordinal so the same pod template serves 1 or N
# brokers, then hands over to the stock apache/kafka entrypoint. Lives in a ConfigMap (not inline in
# `command:`) because Kubernetes expands $(...) in command/args, which would mangle shell arithmetic.
#
# Inputs (env): POD_NAME, POD_NAMESPACE (downward API), REALY_KAFKA_REPLICAS (number of brokers),
#               REALY_KAFKA_HEADLESS (headless Service name), REALY_KAFKA_DOMAIN (cluster DNS suffix).
set -euo pipefail
name="${POD_NAME:?POD_NAME not set}"
ns="${POD_NAMESPACE:?POD_NAMESPACE not set}"
svc="${REALY_KAFKA_HEADLESS:-realy-kafka-headless}"
domain="${REALY_KAFKA_DOMAIN:-svc.cluster.local}"
replicas="${REALY_KAFKA_REPLICAS:-1}"
ord="${name##*-}"
base="${name%-*}"
fqdn() { echo "${base}-$1.${svc}.${ns}.${domain}"; }

export KAFKA_NODE_ID=$((ord + 1))
# clients get this per-pod address back from bootstrap, so it must be unique and resolvable everywhere
self="$(fqdn "$ord")"
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${self}:9092"
voters=""
for i in $(seq 0 $((replicas - 1))); do
  voters="${voters:+$voters,}$((i + 1))@$(fqdn "$i"):9093"
done
export KAFKA_CONTROLLER_QUORUM_VOTERS="$voters"
echo "realy-kafka: node ${KAFKA_NODE_ID}/${replicas}, advertised ${KAFKA_ADVERTISED_LISTENERS}, voters ${voters}"
exec /__cacert_entrypoint.sh /etc/kafka/docker/run
