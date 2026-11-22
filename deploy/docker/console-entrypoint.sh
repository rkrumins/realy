#!/bin/sh
# Console / bench-driver entrypoint. Keeps the writable scenario directory (a volume, so scenarios
# saved from the visual builder survive container restarts) in step with the bundled packs: new ones
# are added, unedited ones follow the image, edited ones are kept (and flagged in the console when the
# bundled version changed); see python/realy/control/scenario_seed.py. Then runs `pulsebench <args>`
# (default: the console).
set -e
mkdir -p "${REALY_SCENARIOS:-/data/scenarios}" "${REALY_RUNS:-/runs}" "${REALY_DATA:-/data}"
python -m realy.control.scenario_seed /opt/realy/scenarios "${REALY_SCENARIOS:-/data/scenarios}"
exec pulsebench "$@"
