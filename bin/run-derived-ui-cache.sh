#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOCK_FILE=${BUS_DERIVED_UI_CACHE_LOCK_FILE:-/home/eljah/data/buscrawl/derived-ui-cache.lock}
exec 8>"$LOCK_FILE"
if ! flock -n 8; then
  echo "$(date -Is) derived UI cache skipped: another UI cache refresh is running"
  exit 0
fi

echo "$(date -Is) derived UI cache refresh started"

if [[ "${BUS_REFRESH_STOP_LAST_PASS_CACHE:-true}" == "true" ]]; then
  BUS_STOP_LAST_PASS_DAILY_ONLY=true \
  BUS_STOP_LAST_PASS_SPARK_MASTER="${BUS_STOP_LAST_PASS_SPARK_MASTER:-local[1]}" \
  BUS_STOP_LAST_PASS_DRIVER_MEMORY="${BUS_STOP_LAST_PASS_DRIVER_MEMORY:-7g}" \
  BUS_STOP_LAST_PASS_EXECUTOR_MEMORY="${BUS_STOP_LAST_PASS_EXECUTOR_MEMORY:-7g}" \
    ./bin/run-stop-last-pass-aggregation.sh

  BUS_STOP_LAST_PASS_RENDER_TILES="${BUS_STOP_LAST_PASS_RENDER_TILES:-false}" \
    ./bin/run-stop-last-pass-cache.sh
fi

if [[ "${BUS_REFRESH_OVERTAKE_CACHE:-true}" == "true" ]]; then
  ./bin/run-overtake-cache.sh
fi

if [[ "${BUS_REFRESH_RUBBERINESS_CACHE:-true}" == "true" ]]; then
  ./bin/run-rubberiness-cache.sh
fi

if [[ "${BUS_REFRESH_SPEED_CACHE:-false}" == "true" ]]; then
  ./bin/run-speed-map-aggregation.sh
  ./bin/run-speed-coordinate-buckets.sh
  ./bin/run-speed-map-cache.sh
fi

echo "$(date -Is) derived UI cache refresh finished"
