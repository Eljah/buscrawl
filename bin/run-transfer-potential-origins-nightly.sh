#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

CITY_TZ=${BUS_CITY_TIMEZONE:-Europe/Moscow}
TARGET_DATE=${BUS_TRANSFER_TARGET_DATE:-$(TZ="$CITY_TZ" date -d 'yesterday' +%F)}
CONFIG_FILE=${BUS_ACCESSIBILITY_ORIGIN_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-origins-config.json}
INDEX_FILE=${BUS_DASHBOARD_ACCESSIBILITY_MAP_INDEX_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-index.json}
ORIGIN_ROOT=${BUS_TRANSFER_POTENTIAL_ORIGIN_ROOT:-/home/eljah/data/buscrawl/transfer-potential-accessibility-origins}
LOCK_FILE=${BUS_TRANSFER_POTENTIAL_LOCK_FILE:-/home/eljah/data/buscrawl/transfer-potential-origin.lock}

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "$(date -Is) origin transfer skipped: previous run is still active"
  exit 0
fi

mkdir -p "$ORIGIN_ROOT"

origin_lines=$(python3 - "$CONFIG_FILE" "$INDEX_FILE" <<'PY'
import json, pathlib, sys
config_path=pathlib.Path(sys.argv[1])
index_path=pathlib.Path(sys.argv[2])
source = json.load(open(config_path, encoding='utf-8')) if config_path.exists() else json.load(open(index_path, encoding='utf-8'))
for item in source.get('origins') or []:
    if item.get('enabled', True) is False:
        continue
    slug=str(item.get('slug') or '').strip()
    label=str(item.get('label') or slug).strip()
    stop_ids=[str(x).strip() for x in (item.get('stopIds') or []) if str(x).strip()]
    if slug and stop_ids:
        print(slug + '\t' + label + '\t' + ','.join(stop_ids))
PY
)

if [[ -z "$origin_lines" ]]; then
  echo "$(date -Is) origin transfer skipped: no enabled origins in $CONFIG_FILE / $INDEX_FILE"
  exit 0
fi

echo "$(date -Is) origin transfer nightly started root=$ORIGIN_ROOT targetDate=$TARGET_DATE"
while IFS=$'\t' read -r slug label stop_ids; do
  [[ -n "$slug" ]] || continue
  origin_dir="$ORIGIN_ROOT/$slug"
  mkdir -p "$origin_dir"
  echo "$(date -Is) origin transfer start slug=$slug label=$label stopIds=$stop_ids"
  BUS_TRANSFER_POTENTIAL_DIR="$origin_dir" \
  BUS_TRANSFER_POTENTIAL_STATE_FILE="$origin_dir/aggregation-state.json" \
  BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/transfer-potential-origin-spark-temp/$slug" \
  BUS_TRANSFER_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_TRANSFER_TARGET_DATE="$TARGET_DATE" \
  BUS_TRANSFER_MAX_BUCKETS_PER_RUN="${BUS_TRANSFER_MAX_BUCKETS_PER_ORIGIN_RUN:-100000}" \
  BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME="${BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME:-23:59}" \
  ./bin/run-transfer-potential.sh
  echo "$(date -Is) origin transfer finish slug=$slug"
done <<< "$origin_lines"
echo "$(date -Is) origin transfer nightly finished"
