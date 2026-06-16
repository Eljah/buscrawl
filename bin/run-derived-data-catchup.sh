#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
exec 9>"$LOCK_FILE"
flock 9

LOG_PREFIX="$(date -Is)"
echo "$LOG_PREFIX derived data catchup started"

for batch in $(seq 1 "${BUS_DERIVED_COMPACTION_MAX_BATCHES:-8}"); do
  batch_log="$(mktemp)"
  BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN="${BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN:-1000}" \
  BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS="${BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS:-8}" \
    ./bin/run-raw-parquet-compaction.sh | tee "$batch_log"

  if grep -Eq "no new parquet files to (process|compact)" "$batch_log"; then
    rm -f "$batch_log"
    break
  fi
  rm -f "$batch_log"
  echo "$(date -Is) raw parquet compaction batch $batch completed"
done

for batch in $(seq 1 "${BUS_DERIVED_STOP_VISIT_MAX_BATCHES:-20}"); do
  batch_log="$(mktemp)"
  BUS_PARQUET_DIR="${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}" \
  BUS_STOP_LAST_PASS_INPUT_HAS_SOURCE_FILE=true \
  BUS_STOP_LAST_PASS_VISITS_ONLY=true \
  BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN="${BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN:-1}" \
  BUS_STOP_LAST_PASS_SPARK_MASTER="${BUS_STOP_LAST_PASS_SPARK_MASTER:-local[1]}" \
  BUS_STOP_LAST_PASS_DRIVER_MEMORY="${BUS_STOP_LAST_PASS_DRIVER_MEMORY:-7g}" \
  BUS_STOP_LAST_PASS_EXECUTOR_MEMORY="${BUS_STOP_LAST_PASS_EXECUTOR_MEMORY:-7g}" \
    ./bin/run-stop-last-pass-aggregation.sh | tee "$batch_log"

  if grep -q "no new parquet files to process" "$batch_log"; then
    rm -f "$batch_log"
    break
  fi
  rm -f "$batch_log"
  echo "$(date -Is) stop visits compacted batch $batch completed"
done

BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN="${BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN:-100000}" \
BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER="${BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER:-local[1]}" \
BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY="${BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY:-7g}" \
BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY="${BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY:-7g}" \
BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS="${BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS:-16}" \
BUS_TRAFFIC_BEHAVIOR_EVENTS_ONLY="${BUS_TRAFFIC_BEHAVIOR_EVENTS_ONLY:-true}" \
  ./bin/run-traffic-behavior-aggregation.sh

BUS_SKIP_HEAVY_JOB_LOCK=true ./bin/run-dashboard-cache.sh

echo "$(date -Is) derived data critical catchup finished"
exec 9>&-

if [[ "${BUS_DERIVED_RUN_UI_CACHE_AFTER:-true}" == "true" ]]; then
  mkdir -p logs
  nohup ./bin/run-derived-ui-cache.sh >> logs/derived-ui-cache.log 2>&1 &
  echo "$(date -Is) derived UI cache refresh started in background"
fi

echo "$(date -Is) derived data catchup finished"
