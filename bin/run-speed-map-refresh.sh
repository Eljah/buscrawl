#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOCK_FILE=${BUS_SPEED_MAP_REFRESH_LOCK_FILE:-/home/eljah/data/buscrawl/speed-map-refresh.lock}
exec 8>"$LOCK_FILE"
if ! flock -n 8; then
  echo "$(date -Is) speed map refresh skipped: another refresh is running"
  exit 0
fi

echo "$(date -Is) speed map refresh started"
./bin/run-speed-map-aggregation.sh
./bin/run-speed-coordinate-buckets.sh
export BUS_SPEED_MAP_RENDER_TILES=${BUS_SPEED_MAP_RENDER_TILES:-false}
./bin/run-speed-map-cache.sh
echo "$(date -Is) speed map refresh finished"
