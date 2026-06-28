#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
CONFIG_FILE=${BUS_ACCESSIBILITY_ORIGIN_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-origins-config.json}
ORIGIN_ROOT=${BUS_TRANSFER_POTENTIAL_ORIGIN_ROOT:-/home/eljah/data/buscrawl/transfer-potential-accessibility-origins}
SEGMENT_TRIPS_DIR=${BUS_SEGMENT_TRIPS_DIR:-/home/eljah/data/buscrawl/traffic-behavior/segment-trips}
RENDER_JSON_DIR=${BUS_ACCESSIBILITY_RENDER_JSON_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-origins-render-cache}
TILE_PREFIX_ROOT=${BUS_ACCESSIBILITY_TILE_PREFIX_ROOT:-accessibility-v4-render-cache}
TILE_ROOT_BASE=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
LOCK_FILE=${BUS_ACCESSIBILITY_FULL_BACKFILL_LOCK_FILE:-/home/eljah/data/buscrawl/accessibility-origin-full-backfill.lock}
TRANSFER_LOCK_FILE=${BUS_TRANSFER_POTENTIAL_LOCK_FILE:-/home/eljah/data/buscrawl/transfer-potential-origin.lock}
RUN_ID=${BUS_ACCESSIBILITY_FULL_BACKFILL_RUN_ID:-$(date +%Y%m%d-%H%M%S)}
LOG_PREFIX="accessibility-full-backfill[$RUN_ID]"

mkdir -p "$ORIGIN_ROOT" "$RENDER_JSON_DIR" "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT" /home/eljah/apps/buscrawl/logs
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
DATE_LIST=$(IFS=,; echo "${DATES[*]}")

mapfile -t ORIGINS < <(python3 - "$CONFIG_FILE" <<'PY'
import json, sys
cfg=json.load(open(sys.argv[1], encoding='utf-8'))
for item in cfg.get('origins') or []:
    if item.get('enabled') is not True:
        continue
    slug=str(item.get('slug') or '').strip()
    label=str(item.get('label') or slug).strip()
    ids=','.join(str(x).strip() for x in (item.get('stopIds') or []) if str(x).strip())
    if slug and ids:
        print(slug + '\t' + label + '\t' + ids)
PY
)
if [ "${#ORIGINS[@]}" -eq 0 ]; then
  echo "$(date -Is) $LOG_PREFIX no enabled origins in $CONFIG_FILE"
  exit 1
fi

echo "$(date -Is) $LOG_PREFIX start dates=${DATES[0]}..${DATES[-1]} dateCount=${#DATES[@]} originCount=${#ORIGINS[@]}"

echo "$(date -Is) $LOG_PREFIX waiting for transfer lock $TRANSFER_LOCK_FILE"
exec 8>"$TRANSFER_LOCK_FILE"
flock 8

echo "$(date -Is) $LOG_PREFIX transfer stage start"
for date in "${DATES[@]}"; do
  for line in "${ORIGINS[@]}"; do
    IFS=$'\t' read -r slug label stop_ids <<< "$line"
    origin_dir="$ORIGIN_ROOT/$slug"
    mkdir -p "$origin_dir"
    echo "$(date -Is) $LOG_PREFIX transfer date=$date slug=$slug stopIds=$stop_ids"
    BUS_TRANSFER_POTENTIAL_DIR="$origin_dir" \
    BUS_TRANSFER_POTENTIAL_STATE_FILE="$origin_dir/aggregation-state-full-backfill-$RUN_ID.json" \
    BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/transfer-potential-origin-spark-temp/$slug-full-backfill" \
    BUS_TRANSFER_ORIGIN_STOP_IDS="$stop_ids" \
    BUS_TRANSFER_TARGET_DATE="$date" \
    BUS_TRANSFER_MAX_BUCKETS_PER_RUN=100000 \
    BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME=23:59 \
    BUS_TRANSFER_POTENTIAL_SPARK_MASTER=${BUS_TRANSFER_POTENTIAL_SPARK_MASTER:-local[2]} \
    ./bin/run-transfer-potential.sh
  done
done
flock -u 8
echo "$(date -Is) $LOG_PREFIX transfer stage done"

echo "$(date -Is) $LOG_PREFIX render stage start serviceDates=$DATE_LIST"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  echo "$(date -Is) $LOG_PREFIX render slug=$slug label=$label stopIds=$stop_ids"
  BUS_TRANSFER_POTENTIAL_DIR="$ORIGIN_ROOT/$slug" \
  BUS_ACCESSIBILITY_MAP_CACHE_FILE="$RENDER_JSON_DIR/$slug.json" \
  BUS_ACCESSIBILITY_TILE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_TILE_URL_PREFIX="$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_ORIGIN_STOP="$label" \
  BUS_ACCESSIBILITY_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_ACCESSIBILITY_SERVICE_DATES="$DATE_LIST" \
  BUS_ACCESSIBILITY_DEPARTURE_START=04:00 \
  BUS_ACCESSIBILITY_DEPARTURE_END=23:45 \
  BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES=15 \
  BUS_ACCESSIBILITY_RENDER_MODES=total,totalNormalized,totalLog,walk,stopTransport \
  BUS_ACCESSIBILITY_WALK_CACHE_FILE=/home/eljah/apps/buscrawl/dashboard-cache/accessibility-walk-cache-primitive.bin.gz \
  BUS_ACCESSIBILITY_RENDER_CACHE_FILE=/home/eljah/apps/buscrawl/dashboard-cache/accessibility-render-cache-z11.bin.gz \
  BUS_ACCESSIBILITY_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/accessibility-map-spark-temp/$slug-full-backfill" \
  BUS_ACCESSIBILITY_SPARK_MASTER=${BUS_ACCESSIBILITY_SPARK_MASTER:-local[2]} \
  ./bin/run-accessibility-map-cache.sh
  echo "$(date -Is) $LOG_PREFIX render done slug=$slug"
done
echo "$(date -Is) $LOG_PREFIX render stage done"

echo "$(date -Is) $LOG_PREFIX aggregate stage check"
current_month=$(TZ="$CITY_TZ" date +%Y-%m)
last_date_month=${DATES[-1]:0:7}
if [ "$last_date_month" != "$current_month" ]; then
  BUS_ACCESSIBILITY_AGGREGATE_MONTH="$last_date_month" ./bin/run-accessibility-normalized-aggregate.sh
  echo "$(date -Is) $LOG_PREFIX aggregate stage done month=$last_date_month"
else
  echo "$(date -Is) $LOG_PREFIX aggregate stage skipped: $last_date_month is current/incomplete month"
fi

echo "$(date -Is) $LOG_PREFIX finished"
