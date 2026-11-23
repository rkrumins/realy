#!/usr/bin/env bash
# Write the secured overlay's passwords (docker-compose.secured.yml) with random values, once. The file is
# git-ignored and readable by its owner only; an existing file is kept, so the stack's data stays usable.
#   deploy/compose/secured-env.sh [path]      (default: deploy/compose/secured.env; `make secured-env`)
set -euo pipefail
out="${1:-deploy/compose/secured.env}"
if [ -f "$out" ]; then
  echo "$out exists (kept)"
  exit 0
fi
umask 077
{
  echo "# docker-compose.secured.yml passwords, generated $(date -u +%Y-%m-%dT%H:%M:%SZ). Git-ignored: never commit."
  for v in SECURED_KAFKA_PASSWORD SECURED_REDIS_PASSWORD SECURED_CLICKHOUSE_PASSWORD; do
    echo "$v=$(openssl rand -hex 16)"
  done
} > "$out"
echo "wrote $out"
