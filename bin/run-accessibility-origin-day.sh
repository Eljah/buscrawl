#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
TARGET_DATE=${BUS_ACCESSIBILITY_TARGET_DATE:-${BUS_TRANSFER_TARGET_DATE:-$(TZ="$CITY_TZ" date -d 'yesterday' +%F)}}
CONFIG_FILE=${BUS_ACCESSIBILITY_ORIGIN_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-origins-config.json}
INDEX_FILE=${BUS_DASHBOARD_ACCESSIBILITY_MAP_INDEX_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-index.json}
ORIGIN_ROOT=${BUS_TRANSFER_POTENTIAL_ORIGIN_ROOT:-/home/eljah/data/buscrawl/transfer-potential-accessibility-origins}
RENDER_JSON_DIR=${BUS_ACCESSIBILITY_RENDER_JSON_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-origins-render-cache}
CONTOUR_STATS_ROOT=${BUS_ACCESSIBILITY_CONTOUR_STATS_ROOT:-/home/eljah/data/buscrawl/accessibility-contour-stats}
TILE_PREFIX_ROOT=${BUS_ACCESSIBILITY_TILE_PREFIX_ROOT:-accessibility-v4-render-cache}
TILE_ROOT_BASE=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
TRANSFER_LOCK_FILE=${BUS_TRANSFER_POTENTIAL_LOCK_FILE:-/home/eljah/data/buscrawl/transfer-potential-origin.lock}
DAY_LOCK_FILE=${BUS_ACCESSIBILITY_DAY_LOCK_FILE:-/home/eljah/data/buscrawl/accessibility-origin-day.lock}
RETENTION_DAYS=${BUS_TRANSFER_POTENTIAL_RETENTION_DAYS:-92}
LOG_PREFIX=${BUS_ACCESSIBILITY_LOG_PREFIX:-accessibility-origin-day}

mkdir -p "$ORIGIN_ROOT" "$RENDER_JSON_DIR" "$CONTOUR_STATS_ROOT" "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT"

exec 9>"$DAY_LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) $LOG_PREFIX skipped targetDate=$TARGET_DATE: previous day run is still active"
  exit 0
fi

mapfile -t ORIGINS < <(python3 - "$CONFIG_FILE" "$INDEX_FILE" <<'PY'
import json, pathlib, sys
config_path=pathlib.Path(sys.argv[1])
index_path=pathlib.Path(sys.argv[2])
if config_path.exists():
    source=json.load(open(config_path, encoding='utf-8'))
elif index_path.exists():
    source=json.load(open(index_path, encoding='utf-8'))
else:
    source={"origins":[]}
for item in source.get('origins') or []:
    if item.get('enabled', True) is False:
        continue
    slug=str(item.get('slug') or '').strip()
    label=str(item.get('label') or slug).strip()
    ids=','.join(str(x).strip() for x in (item.get('stopIds') or []) if str(x).strip())
    if slug and ids:
        print(slug + '\t' + label + '\t' + ids)
PY
)

if [ "${#ORIGINS[@]}" -eq 0 ]; then
  echo "$(date -Is) $LOG_PREFIX skipped targetDate=$TARGET_DATE: no enabled origins"
  exit 0
fi

echo "$(date -Is) $LOG_PREFIX start targetDate=$TARGET_DATE originCount=${#ORIGINS[@]}"

echo "$(date -Is) $LOG_PREFIX waiting for transfer lock $TRANSFER_LOCK_FILE"
exec 8>"$TRANSFER_LOCK_FILE"
flock 8
echo "$(date -Is) $LOG_PREFIX transfer stage start targetDate=$TARGET_DATE"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  origin_dir="$ORIGIN_ROOT/$slug"
  mkdir -p "$origin_dir"
  echo "$(date -Is) $LOG_PREFIX transfer start date=$TARGET_DATE slug=$slug stopIds=$stop_ids"
  BUS_TRANSFER_POTENTIAL_DIR="$origin_dir" \
  BUS_TRANSFER_POTENTIAL_STATE_FILE="$origin_dir/aggregation-state.json" \
  BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/transfer-potential-origin-spark-temp/$slug" \
  BUS_TRANSFER_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_TRANSFER_TARGET_DATE="$TARGET_DATE" \
  BUS_TRANSFER_MAX_BUCKETS_PER_RUN="${BUS_TRANSFER_MAX_BUCKETS_PER_ORIGIN_RUN:-100000}" \
  BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME="${BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME-23:59}" \
  ./bin/run-transfer-potential.sh
  echo "$(date -Is) $LOG_PREFIX transfer finish date=$TARGET_DATE slug=$slug"
done
flock -u 8
echo "$(date -Is) $LOG_PREFIX transfer stage done targetDate=$TARGET_DATE"

echo "$(date -Is) $LOG_PREFIX render stage start targetDate=$TARGET_DATE"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  if [ ! -d "$ORIGIN_ROOT/$slug/journeys" ]; then
    echo "$(date -Is) $LOG_PREFIX render skipped date=$TARGET_DATE slug=$slug: transfer journeys are not available"
    continue
  fi
  echo "$(date -Is) $LOG_PREFIX render start date=$TARGET_DATE slug=$slug label=$label stopIds=$stop_ids"
  BUS_TRANSFER_POTENTIAL_DIR="$ORIGIN_ROOT/$slug" \
  BUS_ACCESSIBILITY_MAP_CACHE_FILE="$RENDER_JSON_DIR/$slug.json" \
  BUS_ACCESSIBILITY_CONTOUR_STATS_DIR="$CONTOUR_STATS_ROOT/originSlug=$slug/serviceDate=$TARGET_DATE" \
  BUS_ACCESSIBILITY_ORIGIN_SLUG="$slug" \
  BUS_ACCESSIBILITY_TILE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_TILE_BASE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_TILE_URL_PREFIX="$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_ORIGIN_STOP="$label" \
  BUS_ACCESSIBILITY_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_ACCESSIBILITY_SERVICE_DATES="$TARGET_DATE" \
  BUS_ACCESSIBILITY_DEPARTURE_START=04:00 \
  BUS_ACCESSIBILITY_DEPARTURE_END=23:45 \
  BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES=15 \
  BUS_ACCESSIBILITY_RENDER_MODES="${BUS_ACCESSIBILITY_RENDER_MODES:-total,totalNormalized,totalLog,walk,stopTransport}" \
  BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM="${BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM:-11}" \
  BUS_ACCESSIBILITY_WALK_CACHE_FILE=${BUS_ACCESSIBILITY_WALK_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-walk-cache-primitive.bin.gz} \
  BUS_ACCESSIBILITY_RENDER_CACHE_FILE=${BUS_ACCESSIBILITY_RENDER_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-render-cache-z11.bin.gz} \
  BUS_ACCESSIBILITY_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/accessibility-map-spark-temp/$slug-$TARGET_DATE" \
  BUS_ACCESSIBILITY_SPARK_MASTER=${BUS_ACCESSIBILITY_SPARK_MASTER:-local[2]} \
  ./bin/run-accessibility-map-cache.sh
  echo "$(date -Is) $LOG_PREFIX render finish date=$TARGET_DATE slug=$slug"
done
echo "$(date -Is) $LOG_PREFIX render stage done targetDate=$TARGET_DATE"

target_month=${TARGET_DATE:0:7}
current_month=$(TZ="$CITY_TZ" date +%Y-%m)
last_day_of_month=$(TZ="$CITY_TZ" date -d "$TARGET_DATE +1 day" +%d)
if [ "$target_month" != "$current_month" ] && [ "$last_day_of_month" = "01" ]; then
  echo "$(date -Is) $LOG_PREFIX aggregate start month=$target_month"
  BUS_ACCESSIBILITY_AGGREGATE_MONTH="$target_month" ./bin/run-accessibility-normalized-aggregate.sh
  echo "$(date -Is) $LOG_PREFIX aggregate finish month=$target_month"
else
  echo "$(date -Is) $LOG_PREFIX aggregate skipped targetDate=$TARGET_DATE targetMonth=$target_month currentMonth=$current_month"
fi

cutoff=$(TZ="$CITY_TZ" date -d "$TARGET_DATE -$RETENTION_DAYS days" +%F)
echo "$(date -Is) $LOG_PREFIX cleanup transfer-potential olderThan=$cutoff retentionDays=$RETENTION_DAYS"
for sub in journeys journey-fragments request-grid-counts; do
  find "$ORIGIN_ROOT" -mindepth 3 -maxdepth 3 -path "*/$sub/serviceDate=*" -type d | while read -r dir; do
    service_date=${dir##*serviceDate=}
    if [[ "$service_date" < "$cutoff" ]]; then
      rm -rf "$dir"
      echo "$(date -Is) $LOG_PREFIX cleanup removed $dir"
    fi
  done
done

echo "$(date -Is) $LOG_PREFIX finished targetDate=$TARGET_DATE"
