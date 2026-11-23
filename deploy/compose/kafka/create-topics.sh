#!/bin/bash
# Create the contract topics (contracts/kafka/topics.yaml). Idempotent (--if-not-exists).
# Compose: one broker, RF 1. Kubernetes 3-broker quorum: REPLICATION_FACTOR=3 MIN_ISR=2, so acks=all
# writes survive the loss of one broker.
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
B="${BOOTSTRAP:-kafka:29092}"
P="${PARTITIONS:-12}"
RF="${REPLICATION_FACTOR:-1}"
ISR="${MIN_ISR:-1}"

mk() {
  local topic="$1" parts="$2"
  shift 2
  local cfg=()
  for c in "$@"; do cfg+=(--config "$c"); done
  if [ "$RF" -gt 1 ]; then cfg+=(--config "min.insync.replicas=$ISR"); fi
  "$K" --bootstrap-server "$B" --create --if-not-exists --topic "$topic" --partitions "$parts" \
    --replication-factor "$RF" "${cfg[@]}"
}

mk payments.events.raw "$P" compression.type=lz4 retention.ms=21600000
mk payments.events.assembled "$P" compression.type=lz4 retention.ms=21600000
mk payments.decisions "$P" message.timestamp.type=LogAppendTime retention.ms=86400000
mk payments.labels 4 retention.ms=604800000
mk payments.events.dlq 6 retention.ms=1209600000
mk payments.devices.telemetry "$P" compression.type=lz4 retention.ms=21600000
mk payments.payees.listings 12 compression.type=lz4 retention.ms=21600000
mk bench.control 1 cleanup.policy=compact
mk bench.metrics 3 retention.ms=86400000
"$K" --bootstrap-server "$B" --list
