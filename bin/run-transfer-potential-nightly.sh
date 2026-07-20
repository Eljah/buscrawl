#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
LOCK_FILE=${BUS_TRANSFER_POTENTIAL_LOCK_FILE:-/home/eljah/data/buscrawl/transfer-potential.lock}
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) transfer potential skipped: previous run is still active"
  exit 0
fi

TARGET_DATE=${BUS_TRANSFER_TARGET_DATE:-$(TZ="$CITY_TZ" date -d 'yesterday' +%F)}
echo "$(date -Is) origin-specific accessibility nightly catchup started endDate=$TARGET_DATE"
BUS_ACCESSIBILITY_CATCHUP_END_DATE="$TARGET_DATE" \
BUS_ACCESSIBILITY_LOG_PREFIX="accessibility-nightly" \
./bin/run-accessibility-origin-selected-backfill.sh
echo "$(date -Is) origin-specific accessibility nightly catchup finished endDate=$TARGET_DATE"
