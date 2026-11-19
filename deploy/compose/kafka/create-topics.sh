#!/bin/bash
# Create the contract topics (contracts/kafka/topics.yaml) on the single-broker dev stack.
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
B="${BOOTSTRAP:-kafka:29092}"
P="${PARTITIONS:-12}"

mk() {
  local topic="$1" parts="$2"
  shift 2
  local cfg=()
  for c in "$@"; do cfg+=(--config "$c"); done
  "$K" --bootstrap-server "$B" --create --if-not-exists --topic "$topic" --partitions "$parts" \
    --replication-factor 1 "${cfg[@]}"
}

mk payments.events.raw "$P" compression.type=lz4 retention.ms=21600000
mk payments.decisions "$P" message.timestamp.type=LogAppendTime retention.ms=86400000
mk payments.labels 4 retention.ms=604800000
mk payments.features "$P" retention.ms=3600000
mk bench.control 1 cleanup.policy=compact
mk bench.metrics 3 retention.ms=86400000
"$K" --bootstrap-server "$B" --list
