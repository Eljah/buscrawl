#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
CURRENT_HOUR=$(TZ="$CITY_TZ" date +%H)
if (( 10#$CURRENT_HOUR < 0 || 10#$CURRENT_HOUR >= 4 )); then
  echo "$(date -Is) transfer potential skipped: outside 00:00-04:00 $CITY_TZ"
  exit 0
fi

LOCK_FILE=${BUS_TRANSFER_POTENTIAL_LOCK_FILE:-/home/eljah/data/buscrawl/transfer-potential.lock}
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) transfer potential skipped: previous run is still active"
  exit 0
fi

echo "$(date -Is) transfer potential nightly started"
./bin/run-transfer-potential.sh
echo "$(date -Is) transfer potential nightly finished"
