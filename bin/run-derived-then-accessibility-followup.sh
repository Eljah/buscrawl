#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl
mkdir -p logs

echo "$(date -Is) followup waiting for current derived-data-catchup.service"
while true; do
  state=$(systemctl show -p ActiveState --value buscrawl-derived-data-catchup.service 2>/dev/null || echo unknown)
  sub=$(systemctl show -p SubState --value buscrawl-derived-data-catchup.service 2>/dev/null || echo unknown)
  echo "$(date -Is) followup observed derived state=$state sub=$sub"
  case "$state" in
    inactive|failed) break ;;
  esac
  sleep 60
done

if pgrep -f 'BusStopLastPassAggregationJob|BusTrafficBehaviorAggregationJob|run-derived-data-catchup.sh' >/dev/null 2>&1; then
  echo "$(date -Is) followup abort: derived process still exists"
  exit 3
fi

echo "$(date -Is) followup extended derived catchup start"
BUS_DERIVED_SKIP_RAW_COMPACTION=true \
BUS_DERIVED_STOP_VISIT_MAX_BATCHES=200 \
BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN=1 \
BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN=100000 \
  ./bin/run-derived-data-catchup.sh

echo "$(date -Is) followup accessibility selected backfill start"
BUS_ACCESSIBILITY_WAIT_DAY_LOCK=true \
BUS_ACCESSIBILITY_LOG_PREFIX='accessibility-selected-followup' \
  ./bin/run-accessibility-origin-selected-backfill.sh

echo "$(date -Is) followup finished"
