#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

CONFIG_FILE=${BUS_ACCESSIBILITY_ORIGIN_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-origins-config.json}
INDEX_FILE=${BUS_DASHBOARD_ACCESSIBILITY_MAP_INDEX_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-index.json}
ORIGIN_ROOT=${BUS_TRANSFER_POTENTIAL_ORIGIN_ROOT:-/home/eljah/data/buscrawl/transfer-potential-accessibility-origins}
RENDER_JSON_DIR=${BUS_ACCESSIBILITY_RENDER_JSON_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-origins-render-cache}
CONTOUR_STATS_ROOT=${BUS_ACCESSIBILITY_CONTOUR_STATS_ROOT:-/home/eljah/data/buscrawl/accessibility-contour-stats}
TILE_PREFIX_ROOT=${BUS_ACCESSIBILITY_TILE_PREFIX_ROOT:-accessibility-v4-render-cache}
TILE_ROOT_BASE=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
LOG_PREFIX=${BUS_ACCESSIBILITY_LOG_PREFIX:-accessibility-json-restore}
DATES=${BUS_ACCESSIBILITY_RESTORE_DATES:-auto}
LOCK_FILE=${BUS_ACCESSIBILITY_RESTORE_LOCK_FILE:-/home/eljah/data/buscrawl/accessibility-json-restore.lock}
ORIGIN_SLUGS=${BUS_ACCESSIBILITY_ORIGIN_SLUGS:-}

mkdir -p "$RENDER_JSON_DIR" "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT"
exec 9>"$LOCK_FILE"
if [ "${BUS_ACCESSIBILITY_RESTORE_WAIT_LOCK:-false}" = "true" ]; then
  echo "$(date -Is) $LOG_PREFIX waiting for restore lock $LOCK_FILE"
  flock 9
else
  if ! flock -n 9; then
    echo "$(date -Is) $LOG_PREFIX skipped: previous restore is still active"
    exit 0
  fi
fi

mapfile -t ORIGINS < <(python3 - "$CONFIG_FILE" "$INDEX_FILE" "$ORIGIN_SLUGS" <<'PY'
import json, pathlib, sys

config_path = pathlib.Path(sys.argv[1])
index_path = pathlib.Path(sys.argv[2])
wanted = {x.strip() for x in sys.argv[3].split(",") if x.strip()}
if config_path.exists():
    source = json.load(open(config_path, encoding="utf-8"))
elif index_path.exists():
    source = json.load(open(index_path, encoding="utf-8"))
else:
    source = {"origins": []}

for item in source.get("origins") or []:
    if item.get("enabled", True) is False:
        continue
    slug = str(item.get("slug") or "").strip()
    label = str(item.get("label") or slug).strip()
    ids = ",".join(str(x).strip() for x in (item.get("stopIds") or []) if str(x).strip())
    if wanted and slug not in wanted:
        continue
    if slug and ids:
        print(slug + "\t" + label + "\t" + ids)
PY
)

echo "$(date -Is) $LOG_PREFIX start dates=$DATES originCount=${#ORIGINS[@]}"
for line in "${ORIGINS[@]}"; do
  IFS=$'\t' read -r slug label stop_ids <<< "$line"
  if [ ! -d "$ORIGIN_ROOT/$slug/journeys" ]; then
    echo "$(date -Is) $LOG_PREFIX skip slug=$slug: no transfer dir"
    continue
  fi
  available_dates=$(python3 - "$ORIGIN_ROOT/$slug/journeys" "$ORIGIN_ROOT/$slug/journey-fragments" "$ORIGIN_ROOT/$slug/request-grid-counts" "$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" "$DATES" <<'PY'
import pathlib, sys

root = pathlib.Path(sys.argv[1])
fragments_root = pathlib.Path(sys.argv[2])
counts_root = pathlib.Path(sys.argv[3])
tile_root = pathlib.Path(sys.argv[4])
requested = [x.strip() for x in sys.argv[5].split(",") if x.strip()]

def dates_with_parquet(base):
    dates = set()
    for item in base.glob("serviceDate=*"):
        if not item.is_dir() or "=" not in item.name:
            continue
        if any(item.rglob("*.parquet")):
            dates.add(item.name.split("=", 1)[1])
    return dates

complete_dates = dates_with_parquet(root) & dates_with_parquet(fragments_root) & dates_with_parquet(counts_root)
tile_dates = {
    item.name[:10]
    for item in tile_root.glob("????-??-??-????")
    if item.is_dir()
}

if requested and requested != ["auto"]:
    wanted = requested
elif tile_dates:
    # Restore JSON only for dates that already have rendered tiles and can be
    # validated from a complete transfer-potential dataset.
    wanted = sorted(tile_dates)
else:
    wanted = sorted(complete_dates)

print(",".join(d for d in wanted if d in complete_dates))
PY
)
  if [ -z "$available_dates" ]; then
    echo "$(date -Is) $LOG_PREFIX skip slug=$slug: none of requested dates has complete transfer partitions"
    continue
  fi
  echo "$(date -Is) $LOG_PREFIX render start slug=$slug dates=$available_dates"
  set +e
  BUS_TRANSFER_POTENTIAL_DIR="$ORIGIN_ROOT/$slug" \
  BUS_ACCESSIBILITY_MAP_CACHE_FILE="$RENDER_JSON_DIR/$slug.json" \
  BUS_ACCESSIBILITY_CONTOUR_STATS_DIR="$CONTOUR_STATS_ROOT/originSlug=$slug" \
  BUS_ACCESSIBILITY_ORIGIN_SLUG="$slug" \
  BUS_ACCESSIBILITY_TILE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_TILE_BASE_ROOT="$TILE_ROOT_BASE/$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_TILE_URL_PREFIX="$TILE_PREFIX_ROOT/$slug" \
  BUS_ACCESSIBILITY_ORIGIN_STOP="$label" \
  BUS_ACCESSIBILITY_ORIGIN_STOP_IDS="$stop_ids" \
  BUS_ACCESSIBILITY_SERVICE_DATES="$available_dates" \
  BUS_ACCESSIBILITY_DEPARTURE_START=04:00 \
  BUS_ACCESSIBILITY_DEPARTURE_END=23:45 \
  BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES=15 \
  BUS_ACCESSIBILITY_RENDER_MODES="${BUS_ACCESSIBILITY_RENDER_MODES:-total,totalNormalized,totalLog,walk,stopTransport}" \
  BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM="${BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM:-11}" \
  BUS_ACCESSIBILITY_WALK_CACHE_FILE=${BUS_ACCESSIBILITY_WALK_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-walk-cache-primitive.bin.gz} \
  BUS_ACCESSIBILITY_RENDER_CACHE_FILE=${BUS_ACCESSIBILITY_RENDER_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-render-cache-z11.bin.gz} \
  BUS_ACCESSIBILITY_SPARK_LOCAL_DIR="/home/eljah/data/buscrawl/accessibility-map-spark-temp/restore-$slug" \
  BUS_ACCESSIBILITY_SPARK_MASTER=${BUS_ACCESSIBILITY_SPARK_MASTER:-local[1]} \
  ./bin/run-accessibility-map-cache.sh
  status=$?
  set -e
  if [ "$status" -eq 0 ]; then
    echo "$(date -Is) $LOG_PREFIX render finish slug=$slug dates=$available_dates"
  else
    echo "$(date -Is) $LOG_PREFIX render failed slug=$slug dates=$available_dates status=$status; continuing"
  fi
done

echo "$(date -Is) $LOG_PREFIX finished dates=$DATES"
