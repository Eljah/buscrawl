#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOG_PREFIX="$(date -Is)"
echo "$LOG_PREFIX derived data catchup started"

BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN="${BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN:-20000}" \
  ./bin/run-raw-parquet-compaction.sh

for batch in $(seq 1 "${BUS_DERIVED_STOP_VISIT_MAX_BATCHES:-20}"); do
  batch_log="$(mktemp)"
  BUS_PARQUET_DIR="${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}" \
  BUS_STOP_LAST_PASS_INPUT_HAS_SOURCE_FILE=true \
  BUS_STOP_LAST_PASS_VISITS_ONLY=true \
  BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN="${BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN:-1}" \
  BUS_STOP_LAST_PASS_SPARK_MASTER="${BUS_STOP_LAST_PASS_SPARK_MASTER:-local[3]}" \
    ./bin/run-stop-last-pass-aggregation.sh | tee "$batch_log"

  if grep -q "no new parquet files to process" "$batch_log"; then
    rm -f "$batch_log"
    break
  fi
  rm -f "$batch_log"
  echo "$(date -Is) stop visits compacted batch $batch completed"
done

BUS_STOP_LAST_PASS_DAILY_ONLY=true \
BUS_STOP_LAST_PASS_SPARK_MASTER="${BUS_STOP_LAST_PASS_SPARK_MASTER:-local[3]}" \
  ./bin/run-stop-last-pass-aggregation.sh

BUS_STOP_LAST_PASS_RENDER_TILES="${BUS_STOP_LAST_PASS_RENDER_TILES:-false}" \
  ./bin/run-stop-last-pass-cache.sh

BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN="${BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN:-100000}" \
BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER="${BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER:-local[3]}" \
BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS="${BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS:-32}" \
  ./bin/run-traffic-behavior-aggregation.sh

./bin/run-overtake-cache.sh
./bin/run-rubberiness-cache.sh
./bin/run-dashboard-cache.sh

echo "$(date -Is) derived data catchup finished"
