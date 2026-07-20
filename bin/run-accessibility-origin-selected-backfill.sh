#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOG_PREFIX=${BUS_ACCESSIBILITY_LOG_PREFIX:-accessibility-origin-selected-backfill}
ORIGIN_SLUGS=${BUS_ACCESSIBILITY_ORIGIN_SLUGS:-}
TARGET_DATES=${BUS_ACCESSIBILITY_TARGET_DATES:-}
SEGMENT_TRIPS_ROOT=${BUS_TRAFFIC_BEHAVIOR_SEGMENT_TRIPS_DIR:-/home/eljah/data/buscrawl/traffic-behavior/segment-trips}
LOCK_FILE=${BUS_ACCESSIBILITY_SELECTED_BACKFILL_LOCK_FILE:-/home/eljah/data/buscrawl/accessibility-origin-selected-backfill.lock}
CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
START_DATE=${BUS_ACCESSIBILITY_CATCHUP_START_DATE:-}
END_DATE=${BUS_ACCESSIBILITY_CATCHUP_END_DATE:-$(TZ="$CITY_TZ" date -d 'yesterday' +%F)}

mkdir -p "$(dirname "$LOCK_FILE")"
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) $LOG_PREFIX skipped: another accessibility selected backfill is active lock=$LOCK_FILE"
  exit 0
fi

if [ -n "$TARGET_DATES" ]; then
  IFS=',' read -r -a DATES <<< "$TARGET_DATES"
else
  mapfile -t DATES < <(find "$SEGMENT_TRIPS_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'serviceDate=*' 2>/dev/null \
    | sed 's|.*/serviceDate=||' \
    | sort \
    | awk -v start="$START_DATE" -v end="$END_DATE" '($0 != "") && (start == "" || $0 >= start) && (end == "" || $0 <= end) { print }')
fi

if [ "${#DATES[@]}" -eq 0 ]; then
  echo "$(date -Is) $LOG_PREFIX failed: no target dates found"
  exit 2
fi

origin_label="$ORIGIN_SLUGS"
if [ -z "$origin_label" ]; then
  origin_label="all-enabled"
fi

echo "$(date -Is) $LOG_PREFIX start origins=$origin_label dateCount=${#DATES[@]} startDate=${START_DATE:-auto} endDate=${END_DATE:-none}"
for target_date in "${DATES[@]}"; do
  target_date="$(echo "$target_date" | xargs)"
  if [ -z "$target_date" ]; then
    continue
  fi
  echo "$(date -Is) $LOG_PREFIX day start targetDate=$target_date origins=$origin_label"
  BUS_ACCESSIBILITY_TARGET_DATE="$target_date" \
  BUS_ACCESSIBILITY_ORIGIN_SLUGS="$ORIGIN_SLUGS" \
  BUS_ACCESSIBILITY_WAIT_DAY_LOCK="${BUS_ACCESSIBILITY_WAIT_DAY_LOCK:-true}" \
  BUS_ACCESSIBILITY_LOG_PREFIX="$LOG_PREFIX/day" \
  ./bin/run-accessibility-origin-day.sh
  echo "$(date -Is) $LOG_PREFIX day finish targetDate=$target_date origins=$origin_label"
done

echo "$(date -Is) $LOG_PREFIX finished origins=$origin_label dateCount=${#DATES[@]}"
