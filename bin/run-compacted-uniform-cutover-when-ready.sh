#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOG_DIR=${BUS_UNIFORM_CUTOVER_LOG_DIR:-/home/eljah/apps/buscrawl/logs}
mkdir -p "$LOG_DIR"

RAW_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
COMPACTED_DIR=${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
STATE_FILE=${BUS_COMPACTED_PARQUET_STATE_FILE:-$COMPACTED_DIR/compaction-state.json}
OUTPUT_DIR=${BUS_UNIFORM_CUTOVER_OUTPUT_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted-uniform-next}
MAX_RAW_BACKLOG=${BUS_UNIFORM_CUTOVER_RAW_BACKLOG_MAX:-2000}
POLL_SECONDS=${BUS_UNIFORM_CUTOVER_POLL_SECONDS:-120}
STABLE_POLLS_REQUIRED=${BUS_UNIFORM_CUTOVER_STABLE_POLLS:-2}

raw_backlog_count() {
  python3 - "$RAW_DIR" "$STATE_FILE" <<'PY'
import json
import os
import sys
from datetime import datetime

raw_dir, state_file = sys.argv[1], sys.argv[2]
try:
    with open(state_file, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    cutoff = datetime.fromisoformat(
        state["lastProcessedModifiedAt"].replace("Z", "+00:00")
    ).timestamp()
except Exception:
    print(10**9)
    raise SystemExit(0)

count = 0
try:
    for name in os.listdir(raw_dir):
        if not name.endswith(".parquet"):
            continue
        path = os.path.join(raw_dir, name)
        try:
            if os.path.getmtime(path) > cutoff:
                count += 1
        except FileNotFoundError:
            pass
except FileNotFoundError:
    count = 10**9
print(count)
PY
}

heavy_processes() {
  pgrep -af 'BusRawParquetCompactionJob|BusStopLastPassAggregationJob|BusTrafficBehaviorAggregationJob|BusDashboardCacheJob|BusCompactedParquetDedupDuckDbJob' \
    | grep -v 'run-compacted-uniform-cutover-when-ready' || true
}

echo "$(date -Is) compacted uniform cutover watcher started"
stable_polls=0
while true; do
  backlog="$(raw_backlog_count)"
  heavy="$(heavy_processes)"
  if [[ -z "$heavy" && "$backlog" -le "$MAX_RAW_BACKLOG" ]]; then
    stable_polls=$((stable_polls + 1))
    echo "$(date -Is) ready poll $stable_polls/$STABLE_POLLS_REQUIRED: raw_backlog=$backlog"
    if [[ "$stable_polls" -ge "$STABLE_POLLS_REQUIRED" ]]; then
      break
    fi
  else
    stable_polls=0
    echo "$(date -Is) waiting: raw_backlog=$backlog"
    if [[ -n "$heavy" ]]; then
      echo "$heavy"
    fi
  fi
  sleep "$POLL_SECONDS"
done

suffix="$(date +%Y%m%d%H%M%S)"
echo "$(date -Is) stopping derived/dashboard timers for compacted cutover"
if [[ -z "${ROOT_PASSWORD:-}" ]]; then
  read -rsp "root password: " ROOT_PASSWORD
  echo
fi
echo "$ROOT_PASSWORD" | su -c 'systemctl stop buscrawl-derived-data-catchup.timer buscrawl-dashboard-cache.timer buscrawl-derived-data-catchup.service buscrawl-dashboard-cache.service || true'

echo "$(date -Is) writing uniform compacted parquet into $OUTPUT_DIR"
rm -rf "$OUTPUT_DIR"
BUS_COMPACTED_DEDUP_INPUT_DIR="$COMPACTED_DIR" \
BUS_COMPACTED_DEDUP_OUTPUT_DIR="$OUTPUT_DIR" \
BUS_COMPACTED_DEDUP_THREADS="${BUS_UNIFORM_CUTOVER_THREADS:-4}" \
BUS_COMPACTED_DEDUP_MEMORY_LIMIT="${BUS_UNIFORM_CUTOVER_MEMORY_LIMIT:-8GB}" \
  ./bin/run-compacted-parquet-dedup-duckdb.sh \
  >> "$LOG_DIR/compacted-uniform-cutover.log" 2>&1

generated="$(ls -td /home/eljah/data/buscrawl/bus-data-parquet-compacted-dedup-tmp/dedup-* | head -1)"
rm -rf "$OUTPUT_DIR"
mv "$generated" "$OUTPUT_DIR"
if [[ -s "$STATE_FILE" ]]; then
  cp "$STATE_FILE" "$OUTPUT_DIR/compaction-state.json"
fi

echo "$(date -Is) swapping compacted parquet directories"
backup="${COMPACTED_DIR}-before-uniform-${suffix}"
mv "$COMPACTED_DIR" "$backup"
mv "$OUTPUT_DIR" "$COMPACTED_DIR"

echo "$(date -Is) compacted cutover complete; backup=$backup"
find "$COMPACTED_DIR" -maxdepth 1 -type f -name 'compact-*.parquet' -print -quit | grep -q . && {
  echo "$(date -Is) ERROR: root compact parquet files remain after cutover"
  exit 1
} || true

echo "$ROOT_PASSWORD" | su -c 'systemctl start buscrawl-derived-data-catchup.timer buscrawl-dashboard-cache.timer buscrawl-derived-data-catchup.service buscrawl-dashboard-cache.service'
echo "$(date -Is) restarted derived/dashboard after compacted cutover"
