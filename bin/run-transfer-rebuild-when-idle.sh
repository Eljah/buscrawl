#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOG_DIR=${BUS_TRANSFER_REBUILD_LOG_DIR:-/home/eljah/apps/buscrawl/logs}
mkdir -p "$LOG_DIR"

RAW_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
COMPACTED_STATE_FILE=${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}
STATIC_GRAPH_DIR=${BUS_TRANSFER_STATIC_GRAPH_DIR:-/home/eljah/data/buscrawl/transfer-static-graph}
STATIC_GRAPH_NEXT=${BUS_TRANSFER_STATIC_GRAPH_NEXT_DIR:-/home/eljah/data/buscrawl/transfer-static-graph-next}
TRANSFER_DIR=${BUS_TRANSFER_POTENTIAL_DIR:-/home/eljah/data/buscrawl/transfer-potential}
MAX_RAW_BACKLOG=${BUS_TRANSFER_WAIT_RAW_BACKLOG_MAX:-2000}
POLL_SECONDS=${BUS_TRANSFER_WAIT_POLL_SECONDS:-300}
STABLE_POLLS_REQUIRED=${BUS_TRANSFER_WAIT_STABLE_POLLS:-2}

heavy_processes() {
  pgrep -af 'BusTrafficBehaviorAggregationJob|BusStopLastPassAggregationJob|BusDashboardCacheJob|BusTransferPotentialJob|BusTransferStaticGraphCacheJob' \
    | grep -v 'run-transfer-rebuild-when-idle' || true
}

raw_backlog_count() {
  python3 - "$RAW_DIR" "$COMPACTED_STATE_FILE" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

raw_dir, state_file = sys.argv[1], sys.argv[2]
try:
    with open(state_file, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    last = state.get("lastProcessedModifiedAt")
    if not last:
        print(10**9)
        raise SystemExit(0)
    cutoff = datetime.fromisoformat(last.replace("Z", "+00:00")).timestamp()
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

echo "$(date -Is) transfer rebuild watcher started"
stable_polls=0
while true; do
  backlog="$(raw_backlog_count)"
  heavy="$(heavy_processes)"
  if [[ -z "$heavy" && "$backlog" -le "$MAX_RAW_BACKLOG" ]]; then
    stable_polls=$((stable_polls + 1))
    echo "$(date -Is) idle poll $stable_polls/$STABLE_POLLS_REQUIRED: raw_backlog=$backlog"
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

echo "$(date -Is) rebuilding static transfer graph cache into $STATIC_GRAPH_NEXT"
rm -rf "$STATIC_GRAPH_NEXT"
BUS_TRANSFER_STATIC_GRAPH_DIR="$STATIC_GRAPH_NEXT" \
  ionice -c2 -n7 nice -n 10 ./bin/run-transfer-static-graph-cache.sh \
  >> "$LOG_DIR/transfer-static-graph-rebuild.log" 2>&1

backup_suffix="$(date +%Y%m%d%H%M%S)"
if [[ -d "$STATIC_GRAPH_DIR" ]]; then
  mv "$STATIC_GRAPH_DIR" "${STATIC_GRAPH_DIR}-before-edges-${backup_suffix}"
fi
mv "$STATIC_GRAPH_NEXT" "$STATIC_GRAPH_DIR"
echo "$(date -Is) static graph cache replaced: $STATIC_GRAPH_DIR"

if [[ -d "$TRANSFER_DIR" ]]; then
  mv "$TRANSFER_DIR" "${TRANSFER_DIR}-before-static-graph-cache-${backup_suffix}"
fi
mkdir -p "$TRANSFER_DIR"

echo "$(date -Is) starting transfer potential rebuild"
BUS_TRANSFER_SEARCH_MODE=static-graph-cache \
BUS_TRANSFER_BACKFILL=true \
BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME=23:59 \
BUS_TRANSFER_MAX_BUCKETS_PER_RUN=${BUS_TRANSFER_MAX_BUCKETS_PER_RUN:-100000} \
  ionice -c2 -n7 nice -n 10 ./bin/run-transfer-potential.sh \
  >> "$LOG_DIR/transfer-potential-static-graph-rebuild.log" 2>&1

echo "$(date -Is) transfer potential rebuild finished"
