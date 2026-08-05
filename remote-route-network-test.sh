#!/bin/bash
set -euo pipefail
cd /home/eljah/apps/buscrawl
rm -rf /tmp/buscrawl-transfer-route-network /tmp/buscrawl-transfer-route-network-spark
mkdir -p /tmp/buscrawl-transfer-route-network
python3 - <<'PY'
import json
keys=[f'2026-06-21|{m}' for m in range(0,24*60,10) if m!=480]
json.dump({'processedBucketKeys':keys,'processedServiceDates':['2026-06-21'],'lastProcessedServiceDate':'2026-06-21','updatedAt':'2026-06-22T00:00:00Z'}, open('/tmp/buscrawl-transfer-route-network/state.json','w'))
PY
BUS_TRANSFER_SEARCH_MODE=route-network \
BUS_TRANSFER_TARGET_DATE=2026-06-21 \
BUS_TRANSFER_POTENTIAL_DIR=/tmp/buscrawl-transfer-route-network \
BUS_TRANSFER_POTENTIAL_STATE_FILE=/tmp/buscrawl-transfer-route-network/state.json \
BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR=/tmp/buscrawl-transfer-route-network-spark \
BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME=23:59 \
BUS_TRANSFER_MAX_BUCKETS_PER_RUN=1 \
BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_ROUTE_PATTERN=12 \
BUS_TRANSFER_POTENTIAL_SPARK_MASTER=local[2] \
BUS_TRANSFER_POTENTIAL_DRIVER_MEMORY=8g \
BUS_TRANSFER_POTENTIAL_EXECUTOR_MEMORY=8g \
./bin/run-transfer-potential.sh
