#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
SEGMENT_TRIPS_DIR=${BUS_SEGMENT_TRIPS_DIR:-/home/eljah/data/buscrawl/traffic-behavior/segment-trips}
LOCK_FILE=${BUS_ACCESSIBILITY_FULL_BACKFILL_LOCK_FILE:-/home/eljah/data/buscrawl/accessibility-origin-full-backfill.lock}
RUN_ID=${BUS_ACCESSIBILITY_FULL_BACKFILL_RUN_ID:-$(date +%Y%m%d-%H%M%S)}
LOG_PREFIX="accessibility-full-backfill[$RUN_ID]"

mkdir -p /home/eljah/apps/buscrawl/logs
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) $LOG_PREFIX another full backfill is already active"
  exit 0
fi

mapfile -t DATES < <(find "$SEGMENT_TRIPS_DIR" -maxdepth 2 -type d -name 'serviceDate=*' | sed 's/.*serviceDate=//' | sort -u)
if [ "${#DATES[@]}" -eq 0 ]; then
  echo "$(date -Is) $LOG_PREFIX no serviceDate partitions under $SEGMENT_TRIPS_DIR"
  exit 1
fi

echo "$(date -Is) $LOG_PREFIX start dates=${DATES[0]}..${DATES[-1]} dateCount=${#DATES[@]}"

for date in "${DATES[@]}"; do
  echo "$(date -Is) $LOG_PREFIX date start $date"
  BUS_ACCESSIBILITY_TARGET_DATE="$date" \
  BUS_ACCESSIBILITY_LOG_PREFIX="$LOG_PREFIX day" \
  ./bin/run-accessibility-origin-day.sh
  echo "$(date -Is) $LOG_PREFIX date done $date"
done

echo "$(date -Is) $LOG_PREFIX finished"
