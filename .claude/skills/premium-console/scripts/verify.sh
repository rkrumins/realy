#!/usr/bin/env bash
# Run every automated gate of a console project (layout: <project>/frontend, <project>/backend).
#   scripts/verify.sh <project-dir>            PYTHON=/path/to/python to pick the interpreter
# Gates: tsc --noEmit, vitest run, vite build, pytest. Prints a pass/fail table; exit 1 on any failure.
set -u
P="${1:-.}"
PY="${PYTHON:-python3}"
declare -a NAMES=() RESULTS=()

gate() {
  local name="$1"; shift
  echo "==> $name"
  if "$@"; then RESULTS+=("pass"); else RESULTS+=("FAIL"); fi
  NAMES+=("$name")
}

if [ -d "$P/frontend" ]; then
  gate "frontend: tsc --noEmit" bash -c "cd '$P/frontend' && npx tsc --noEmit"
  gate "frontend: vitest run"   bash -c "cd '$P/frontend' && npx vitest run"
  gate "frontend: vite build"   bash -c "cd '$P/frontend' && npx vite build"
fi
if [ -d "$P/backend" ]; then
  gate "backend: pytest"        bash -c "cd '$P/backend' && '$PY' -m pytest -q"
fi

echo
fail=0
for i in "${!NAMES[@]}"; do
  printf '  %-28s %s\n' "${NAMES[$i]}" "${RESULTS[$i]}"
  [ "${RESULTS[$i]}" = "pass" ] || fail=1
done
exit $fail
