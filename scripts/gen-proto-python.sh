#!/usr/bin/env bash
# Generate Python protobuf/gRPC stubs from contracts/proto into python/ (package paths follow proto paths).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-python}"
PROTOS=$(cd contracts/proto && find . -name '*.proto' | sed 's|^\./||' | sort)
(cd contracts/proto && "$PY" -m grpc_tools.protoc -I . --python_out="$ROOT/python" --pyi_out="$ROOT/python" \
  --grpc_python_out="$ROOT/python" $PROTOS)
for d in python/realy/payments python/realy/payments/v1 python/realy/features python/realy/features/v1 \
         python/realy/scoring python/realy/scoring/v1 python/realy/bench python/realy/bench/v1 \
         python/kserve python/kserve/v2; do
  touch "$d/__init__.py"
done
echo "generated python stubs for: $PROTOS"
