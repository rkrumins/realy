#!/bin/sh
# Console / bench-driver entrypoint. Seeds the writable scenario directory (a volume, so scenarios
# saved from the visual builder survive container restarts) with the bundled packs, without
# overwriting edited ones, then runs `pulsebench <args>` (default: the console).
set -e
mkdir -p "${REALY_SCENARIOS:-/data/scenarios}" "${REALY_RUNS:-/runs}" "${REALY_DATA:-/data}"
for f in /opt/realy/scenarios/*.yaml; do
  dest="${REALY_SCENARIOS:-/data/scenarios}/$(basename "$f")"
  [ -e "$dest" ] || cp "$f" "$dest"
done
exec pulsebench "$@"
