#!/bin/bash
# Apply generated DDL (contracts/storage/clickhouse) with the Kafka broker list substituted.
set -euo pipefail
BROKERS="${KAFKA_BROKERS:-kafka:29092}"
# the server's own user when one is configured (CLICKHOUSE_USER / CLICKHOUSE_PASSWORD; the secured overlay)
CH=(clickhouse-client --multiquery --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}")
# Contract .proto files become ClickHouse format schemas (ProtobufSingle) for the Kafka engine tables.
mkdir -p /var/lib/clickhouse/format_schemas
cp -r /realy-proto-src/. /var/lib/clickhouse/format_schemas/
for f in /realy-sql/*.sql; do
  echo "applying $f"
  sed "s/__KAFKA_BROKERS__/${BROKERS}/g" "$f" | "${CH[@]}"
done
