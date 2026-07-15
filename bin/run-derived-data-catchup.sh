#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOG_PREFIX="$(date -Is)"
echo "$LOG_PREFIX derived data catchup started"

raw_backlog_count() {
  python3 - \
    "${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}" \
    "${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}" <<'PY'
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

if [[ "${BUS_DERIVED_BACKLOG_SCAN:-false}" == "true" ]]; then
  initial_raw_backlog="$(raw_backlog_count)"
else
  initial_raw_backlog="${BUS_DERIVED_INITIAL_RAW_BACKLOG_OVERRIDE:-0}"
  echo "$(date -Is) raw backlog scan skipped: raw_backlog=$initial_raw_backlog"
fi
fast_backlog_threshold="${BUS_DERIVED_FAST_COMPACTION_BACKLOG_THRESHOLD:-50000}"
skip_downstream_backlog_threshold="${BUS_DERIVED_SKIP_DOWNSTREAM_RAW_BACKLOG_THRESHOLD:-50000}"
if [[ "$initial_raw_backlog" -gt "$fast_backlog_threshold" ]]; then
  compaction_max_batches="${BUS_DERIVED_FAST_COMPACTION_MAX_BATCHES:-10}"
  compaction_max_files_per_run="${BUS_DERIVED_FAST_COMPACTION_MAX_FILES_PER_RUN:-50000}"
  echo "$(date -Is) fast raw compaction mode: raw_backlog=$initial_raw_backlog max_batches=$compaction_max_batches max_files_per_run=$compaction_max_files_per_run"
else
  compaction_max_batches="${BUS_DERIVED_COMPACTION_MAX_BATCHES:-8}"
  compaction_max_files_per_run="${BUS_DERIVED_COMPACTION_MAX_FILES_PER_RUN:-20000}"
  echo "$(date -Is) normal raw compaction mode: raw_backlog=$initial_raw_backlog max_batches=$compaction_max_batches max_files_per_run=$compaction_max_files_per_run"
fi

if [[ "${BUS_DERIVED_SKIP_RAW_COMPACTION:-false}" == "true" ]]; then
  echo "$(date -Is) raw parquet compaction skipped by caller"
else
  for batch in $(seq 1 "$compaction_max_batches"); do
    batch_log="$(mktemp)"
    BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN="$compaction_max_files_per_run" \
    BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS="${BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS:-8}" \
    BUS_SKIP_HEAVY_JOB_LOCK=true \
      ./bin/run-raw-parquet-compaction.sh | tee "$batch_log"

    if grep -Eq "no new parquet files to (process|compact)" "$batch_log"; then
      rm -f "$batch_log"
      break
    fi
    rm -f "$batch_log"
    echo "$(date -Is) raw parquet compaction batch $batch completed"
  done
fi

if [[ "${BUS_DERIVED_RUN_RAW_CLEANUP_AFTER_COMPACTION:-false}" == "true" ]]; then
  if [[ -x ./bin/run-raw-parquet-cleanup.sh ]]; then
    BUS_RAW_PARQUET_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
      ./bin/run-raw-parquet-cleanup.sh
  else
    echo "$(date -Is) raw parquet cleanup script is absent; skipping"
  fi
  if [[ -x ./bin/run-raw-spool-cleanup.sh ]]; then
    BUS_RAW_SPOOL_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
      ./bin/run-raw-spool-cleanup.sh
  else
    echo "$(date -Is) raw spool cleanup script is absent; skipping"
  fi
fi

if [[ "${BUS_DERIVED_BACKLOG_SCAN:-false}" == "true" ]]; then
  remaining_raw_backlog="$(raw_backlog_count)"
  if [[ "$remaining_raw_backlog" -gt "$skip_downstream_backlog_threshold" ]]; then
    echo "$(date -Is) raw compaction still behind: raw_backlog=$remaining_raw_backlog; skipping downstream derived jobs in this pass"
    echo "$(date -Is) derived data catchup finished"
    exit 0
  fi
else
  echo "$(date -Is) raw backlog post-scan skipped"
fi

for batch in $(seq 1 "${BUS_DERIVED_STOP_VISIT_MAX_BATCHES:-100}"); do
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

./bin/run-dashboard-cache.sh

echo "$(date -Is) derived data critical catchup finished"

if [[ "${BUS_DERIVED_RUN_UI_CACHE_AFTER:-true}" == "true" ]]; then
  mkdir -p logs
  nohup ./bin/run-derived-ui-cache.sh >> logs/derived-ui-cache.log 2>&1 &
  echo "$(date -Is) derived UI cache refresh started in background"
fi

echo "$(date -Is) derived data catchup finished"
