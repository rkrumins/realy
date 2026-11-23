#!/usr/bin/env bash
# Generate Python protobuf/gRPC stubs from contracts/proto into python/ (package paths follow proto paths).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-python}"
PROTOS=$(cd contracts/proto && find . -name '*.proto' | sed 's|^\./||' | sort)
(cd contracts/proto && "$PY" -m grpc_tools.protoc -I . --python_out="$ROOT/python" --pyi_out="$ROOT/python" \
  --grpc_python_out="$ROOT/python" $PROTOS)
# every directory that received stubs (and its parents up to python/) is a package
for f in $(cd "$ROOT/python" && find . -name '*_pb2.py' | sed 's|^\./||'); do
  d=$(dirname "$f")
  while [ "$d" != "." ]; do
    touch "python/$d/__init__.py"
    d=$(dirname "$d")
  done
done
echo "generated python stubs for: $PROTOS"
