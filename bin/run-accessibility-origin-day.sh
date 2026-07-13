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
HEAVY_JOB_LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
RETENTION_DAYS=${BUS_TRANSFER_POTENTIAL_RETENTION_DAYS:-92}
LOG_PREFIX=${BUS_ACCESSIBILITY_LOG_PREFIX:-accessibility-origin-day}
ORIGIN_SLUGS=${BUS_ACCESSIBILITY_ORIGIN_SLUGS:-}
FORCE_TRANSFER=${BUS_ACCESSIBILITY_FORCE_TRANSFER:-false}
FORCE_RENDER=${BUS_ACCESSIBILITY_FORCE_RENDER:-false}
WAIT_DAY_LOCK=${BUS_ACCESSIBILITY_WAIT_DAY_LOCK:-false}

mkdir -p "$ORIGIN_ROOT" "$RENDER_JSON_DIR" "$CONTOUR_STATS_ROOT" "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT"

exec 9>"$DAY_LOCK_FILE"
if [ "$WAIT_DAY_LOCK" = "true" ]; then
  echo "$(date -Is) $LOG_PREFIX waiting for day lock $DAY_LOCK_FILE"
  flock 9
else
  if ! flock -n 9; then
    echo "$(date -Is) $LOG_PREFIX skipped targetDate=$TARGET_DATE: previous day run is still active"
    exit 0
  fi
fi

mapfile -t ORIGINS < <(python3 - "$CONFIG_FILE" "$INDEX_FILE" "$ORIGIN_SLUGS" <<'PY'
import json, pathlib, sys
config_path=pathlib.Path(sys.argv[1])
index_path=pathlib.Path(sys.argv[2])
wanted={x.strip() for x in sys.argv[3].split(",") if x.strip()}
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
    if wanted and slug not in wanted:
        continue
    if slug and ids:
        print(slug + '\t' + label + '\t' + ids)
PY
)

if [ "${#ORIGINS[@]}" -eq 0 ]; then
  echo "$(date -Is) $LOG_PREFIX skipped targetDate=$TARGET_DATE: no enabled origins"
  exit 0
fi

echo "$(date -Is) $LOG_PREFIX start targetDate=$TARGET_DATE originCount=${#ORIGINS[@]}"

partition_has_parquet() {
  local dir="$1"
  [ -d "$dir" ] && find "$dir" -type f -name '*.parquet' -print -quit 2>/dev/null | grep -q .
}

transfer_partition_dir() {
  local slug="$1"
  local sub="$2"
  printf '%s/%s/%s/serviceDate=%s' "$ORIGIN_ROOT" "$slug" "$sub" "$TARGET_DATE"
}

transfer_complete() {
  local slug="$1"
  partition_has_parquet "$(transfer_partition_dir "$slug" journeys)" \
    && partition_has_parquet "$(transfer_partition_dir "$slug" journey-fragments)" \
    && partition_has_parquet "$(transfer_partition_dir "$slug" request-grid-counts)"
}

transfer_any() {
  local slug="$1"
  partition_has_parquet "$(transfer_partition_dir "$slug" journeys)" \
    || partition_has_parquet "$(transfer_partition_dir "$slug" journey-fragments)" \
    || partition_has_parquet "$(transfer_partition_dir "$slug" request-grid-counts)"
}

clear_transfer_date() {
  local slug="$1"
  rm -rf \
    "$(transfer_partition_dir "$slug" journeys)" \
    "$(transfer_partition_dir "$slug" journey-fragments)" \
    "$(transfer_partition_dir "$slug" request-grid-counts)"
}

tile_complete() {
  local slug="$1"
  local root="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug"
  [ -d "$root" ] && find "$root" -mindepth 1 -maxdepth 1 -type d -name "$TARGET_DATE-????" -print -quit 2>/dev/null | grep -q .
}

contour_complete() {
  local slug="$1"
  partition_has_parquet "$CONTOUR_STATS_ROOT/originSlug=$slug/serviceDate=$TARGET_DATE"
}

json_has_date() {
  local file="$1"
  [ -f "$file" ] && python3 - "$file" "$TARGET_DATE" <<'PY'
import json, sys
path, target = sys.argv[1], sys.argv[2]
try:
    data = json.load(open(path, encoding="utf-8"))
except Exception:
    sys.exit(1)
dates = set()
for item in data.get("snapshots") or []:
    value = str(item.get("serviceDate") or "")
    if value:
        dates.add(value)
for item in data.get("dates") or []:
    value = str(item.get("serviceDate") or item)
    if value:
        dates.add(value[:10])
sys.exit(0 if target in dates else 1)
PY
}

render_complete() {
  local slug="$1"
  tile_complete "$slug" \
    && json_has_date "$RENDER_JSON_DIR/$slug.json" \
    && contour_complete "$slug"
}

render_departure_times() {
  local slug="$1"
  python3 - \
    "$RENDER_JSON_DIR/$slug.json" \
    "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
    "$CONTOUR_STATS_ROOT/originSlug=$slug/serviceDate=$TARGET_DATE" \
    "$TARGET_DATE" \
    "${BUS_ACCESSIBILITY_RENDER_MODES:-total,totalNormalized,totalLog,walk,stopTransport}" <<'PY'
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta

json_path = pathlib.Path(sys.argv[1])
tile_root = pathlib.Path(sys.argv[2])
contour_dir = pathlib.Path(sys.argv[3])
target_date = sys.argv[4]
mode_names = [x.strip() for x in sys.argv[5].split(",") if x.strip()]
mode_dirs = {
    "total": "total",
    "totalNormalized": "total-normalized",
    "totalLog": "total-log",
    "walk": "walk",
    "stopTransport": "stop-transport",
}
required_mode_dirs = [mode_dirs[m] for m in mode_names if m in mode_dirs]

def time_from_snapshot(snapshot_id):
    prefix = target_date + "-"
    if not snapshot_id.startswith(prefix):
        return None
    value = snapshot_id[len(prefix):]
    if len(value) != 4 or not value.isdigit():
        return None
    return value[:2] + ":" + value[2:]

json_times = set()
if json_path.exists():
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        for item in data.get("snapshots") or []:
            snapshot_id = str(item.get("id") or item.get("snapshotId") or "")
            value = time_from_snapshot(snapshot_id)
            if value:
                json_times.add(value)
    except Exception:
        pass

tile_times = set()
if tile_root.exists():
    try:
        for child in tile_root.iterdir():
            if not child.is_dir():
                continue
            value = time_from_snapshot(child.name)
            if value:
                tile_times.add(value)
    except Exception:
        pass

tile_complete_times = set()
for value in tile_times:
    snapshot_dir = tile_root / (target_date + "-" + value.replace(":", ""))
    complete = True
    for mode_dir in required_mode_dirs:
        if not (snapshot_dir / mode_dir).is_dir():
            complete = False
            break
    if not (snapshot_dir / "_SUCCESS").is_file():
        complete = False
    if complete:
        tile_complete_times.add(value)

observed_times = json_times | tile_times
if not observed_times:
    print("__FULL__")
    raise SystemExit(0)

contour_complete = False
if contour_dir.exists():
    try:
        contour_complete = any(
            child.is_file() and child.name.endswith(".parquet")
            for child in contour_dir.iterdir()
        )
    except Exception:
        contour_complete = False
complete_times = json_times & tile_complete_times
if not contour_complete:
    # Contour stats are partitioned per origin-date, not per tile; rerender observed
    # snapshots to rebuild them without forcing the entire day.
    missing = sorted(observed_times)
else:
    start = min(observed_times)
    end = max(observed_times)
    current = datetime.strptime(start, "%H:%M")
    finish = datetime.strptime(end, "%H:%M")
    expected = []
    while current <= finish:
        expected.append(current.strftime("%H:%M"))
        current += timedelta(minutes=15)
    missing = [value for value in expected if value not in complete_times]

if missing:
    print(",".join(missing))
else:
    print("__SKIP__")
PY
}

echo "$(date -Is) $LOG_PREFIX waiting for transfer lock $TRANSFER_LOCK_FILE"
exec 8>"$TRANSFER_LOCK_FILE"
flock 8
echo "$(date -Is) $LOG_PREFIX transfer stage start targetDate=$TARGET_DATE"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  origin_dir="$ORIGIN_ROOT/$slug"
  mkdir -p "$origin_dir"
  if [ "$FORCE_TRANSFER" != "true" ] && transfer_complete "$slug"; then
    echo "$(date -Is) $LOG_PREFIX transfer skipped date=$TARGET_DATE slug=$slug: complete partitions already exist"
    continue
  fi
  if transfer_any "$slug"; then
    echo "$(date -Is) $LOG_PREFIX transfer cleanup date=$TARGET_DATE slug=$slug: removing incomplete/forced partitions before recompute"
    clear_transfer_date "$slug"
  fi
  echo "$(date -Is) $LOG_PREFIX transfer start date=$TARGET_DATE slug=$slug stopIds=$stop_ids"
  exec 7>"$HEAVY_JOB_LOCK_FILE"
  echo "$(date -Is) $LOG_PREFIX transfer waiting for heavy IO lock $HEAVY_JOB_LOCK_FILE"
  flock 7
  BUS_TRANSFER_POTENTIAL_DIR="$origin_dir" \
  BUS_TRANSFER_POTENTIAL_STATE_FILE="$origin_dir/aggregation-state.json" \
  BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/transfer-potential-origin-spark-temp/$slug" \
  BUS_TRANSFER_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_TRANSFER_TARGET_DATE="$TARGET_DATE" \
  BUS_TRANSFER_MAX_BUCKETS_PER_RUN="${BUS_TRANSFER_MAX_BUCKETS_PER_ORIGIN_RUN:-100000}" \
  BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME="${BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME-23:59}" \
  ./bin/run-transfer-potential.sh
  flock -u 7
  if transfer_complete "$slug"; then
    echo "$(date -Is) $LOG_PREFIX transfer finish date=$TARGET_DATE slug=$slug"
  else
    echo "$(date -Is) $LOG_PREFIX transfer incomplete date=$TARGET_DATE slug=$slug: one or more partitions are missing"
  fi
done
flock -u 8
echo "$(date -Is) $LOG_PREFIX transfer stage done targetDate=$TARGET_DATE"

echo "$(date -Is) $LOG_PREFIX render stage start targetDate=$TARGET_DATE"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  if ! transfer_complete "$slug"; then
    echo "$(date -Is) $LOG_PREFIX render skipped date=$TARGET_DATE slug=$slug: transfer partitions are incomplete"
    continue
  fi
  render_times="__FULL__"
  if [ "$FORCE_RENDER" != "true" ]; then
    render_times="$(render_departure_times "$slug")"
    if [ "$render_times" = "__SKIP__" ]; then
      echo "$(date -Is) $LOG_PREFIX render skipped date=$TARGET_DATE slug=$slug: all observed 15-minute snapshots have tiles, JSON, and contour stats"
      continue
    fi
  fi
  if [ "$render_times" = "__FULL__" ]; then
    echo "$(date -Is) $LOG_PREFIX render start date=$TARGET_DATE slug=$slug label=$label stopIds=$stop_ids scope=full"
  else
    echo "$(date -Is) $LOG_PREFIX render start date=$TARGET_DATE slug=$slug label=$label stopIds=$stop_ids scope=missing departureTimes=$render_times"
  fi
  set +e
  exec 7>"$HEAVY_JOB_LOCK_FILE"
  echo "$(date -Is) $LOG_PREFIX render waiting for heavy IO lock $HEAVY_JOB_LOCK_FILE"
  flock 7
  if [ "$render_times" = "__FULL__" ]; then
    BUS_TRANSFER_POTENTIAL_DIR="$ORIGIN_ROOT/$slug" \
    BUS_ACCESSIBILITY_MAP_CACHE_FILE="$RENDER_JSON_DIR/$slug.json" \
    BUS_ACCESSIBILITY_CONTOUR_STATS_DIR="$CONTOUR_STATS_ROOT/originSlug=$slug/serviceDate=$TARGET_DATE" \
    BUS_ACCESSIBILITY_ORIGIN_SLUG="$slug" \
    BUS_ACCESSIBILITY_TILE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_BASE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_URL_PREFIX="$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_STAGING_ROOT="${BUS_ACCESSIBILITY_TILE_STAGING_ROOT:-/home/eljah/data/buscrawl/accessibility-render-staging/$slug}" \
    BUS_ACCESSIBILITY_SNAPSHOT_RENDER_SLEEP_MILLIS="${BUS_ACCESSIBILITY_SNAPSHOT_RENDER_SLEEP_MILLIS:-3000}" \
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
  else
    BUS_TRANSFER_POTENTIAL_DIR="$ORIGIN_ROOT/$slug" \
    BUS_ACCESSIBILITY_MAP_CACHE_FILE="$RENDER_JSON_DIR/$slug.json" \
    BUS_ACCESSIBILITY_CONTOUR_STATS_DIR="$CONTOUR_STATS_ROOT/originSlug=$slug/serviceDate=$TARGET_DATE" \
    BUS_ACCESSIBILITY_ORIGIN_SLUG="$slug" \
    BUS_ACCESSIBILITY_TILE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_BASE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_URL_PREFIX="$TILE_PREFIX_ROOT/$slug" \
    BUS_ACCESSIBILITY_TILE_STAGING_ROOT="${BUS_ACCESSIBILITY_TILE_STAGING_ROOT:-/home/eljah/data/buscrawl/accessibility-render-staging/$slug}" \
    BUS_ACCESSIBILITY_SNAPSHOT_RENDER_SLEEP_MILLIS="${BUS_ACCESSIBILITY_SNAPSHOT_RENDER_SLEEP_MILLIS:-3000}" \
    BUS_ACCESSIBILITY_ORIGIN_STOP="$label" \
    BUS_ACCESSIBILITY_ORIGIN_STOP_IDS="$stop_ids" \
    BUS_ACCESSIBILITY_SERVICE_DATES="$TARGET_DATE" \
    BUS_ACCESSIBILITY_DEPARTURE_TIMES="$render_times" \
    BUS_ACCESSIBILITY_RENDER_MODES="${BUS_ACCESSIBILITY_RENDER_MODES:-total,totalNormalized,totalLog,walk,stopTransport}" \
    BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM="${BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM:-11}" \
    BUS_ACCESSIBILITY_WALK_CACHE_FILE=${BUS_ACCESSIBILITY_WALK_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-walk-cache-primitive.bin.gz} \
    BUS_ACCESSIBILITY_RENDER_CACHE_FILE=${BUS_ACCESSIBILITY_RENDER_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-render-cache-z11.bin.gz} \
    BUS_ACCESSIBILITY_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/accessibility-map-spark-temp/$slug-$TARGET_DATE" \
    BUS_ACCESSIBILITY_SPARK_MASTER=${BUS_ACCESSIBILITY_SPARK_MASTER:-local[2]} \
    ./bin/run-accessibility-map-cache.sh
  fi
  status=$?
  flock -u 7
  set -e
  if [ "$status" -eq 0 ]; then
    echo "$(date -Is) $LOG_PREFIX render finish date=$TARGET_DATE slug=$slug"
  else
    echo "$(date -Is) $LOG_PREFIX render failed date=$TARGET_DATE slug=$slug status=$status; continuing with next origin"
  fi
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
