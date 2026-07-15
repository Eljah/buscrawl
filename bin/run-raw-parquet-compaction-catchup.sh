#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

log() {
  echo "$(date -Is) raw parquet compaction catchup: $*"
}

max_batches=${BUS_RAW_PARQUET_CATCHUP_MAX_BATCHES:-0}
sleep_seconds=${BUS_RAW_PARQUET_CATCHUP_SLEEP_SECONDS:-30}
cleanup_every_batches=${BUS_RAW_PARQUET_CATCHUP_CLEANUP_EVERY_BATCHES:-1}
empty_sleep_seconds=${BUS_RAW_PARQUET_CATCHUP_EMPTY_SLEEP_SECONDS:-300}
compaction_state_file=${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}
cleanup_state_file=${BUS_RAW_PARQUET_CLEANUP_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-cleanup-state.json}

batch=0
empty_rounds=0
log "started max_batches=$max_batches cleanup_every_batches=$cleanup_every_batches"

cleanup_lags_compaction() {
  /usr/bin/python3 - "$compaction_state_file" "$cleanup_state_file" <<'PY'
import json
import sys
from pathlib import Path

compaction_path = Path(sys.argv[1])
cleanup_path = Path(sys.argv[2])
if not compaction_path.is_file():
    sys.exit(1)
compaction = json.loads(compaction_path.read_text())
cleanup = json.loads(cleanup_path.read_text()) if cleanup_path.is_file() else {}
if int(cleanup.get("legacyManifestOffset", 0)) < int(compaction.get("legacyManifestOffset", 0)):
    sys.exit(0)
if int(cleanup.get("manifestOffset", 0)) < int(compaction.get("manifestOffset", 0)):
    sys.exit(0)
sys.exit(1)
PY
}

run_cleanup() {
  log "cleanup starting"
  BUS_RAW_PARQUET_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
  BUS_RAW_PARQUET_CLEANUP_DRY_RUN="${BUS_RAW_PARQUET_CLEANUP_DRY_RUN:-false}" \
  BUS_RAW_PARQUET_CLEANUP_MAX_FILES="${BUS_RAW_PARQUET_CLEANUP_MAX_FILES:-10000}" \
  BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS="${BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS:-20000}" \
  BUS_RAW_PARQUET_CLEANUP_MIN_AGE_HOURS="${BUS_RAW_PARQUET_CLEANUP_MIN_AGE_HOURS:-1}" \
    ./bin/run-raw-parquet-cleanup.sh
}

while true; do
  batch=$((batch + 1))
  if [[ "$max_batches" -gt 0 && "$batch" -gt "$max_batches" ]]; then
    log "finished by max_batches=$max_batches"
    exit 0
  fi

  batch_log="$(mktemp)"
  log "compaction batch=$batch starting"
  set +e
  BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE="${BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE:-full-history}" \
  BUS_COMPACTED_PARQUET_NEWEST_FIRST="${BUS_COMPACTED_PARQUET_NEWEST_FIRST:-false}" \
  BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN="${BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN:-20000}" \
  BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS="${BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS:-8}" \
  BUS_COMPACTED_PARQUET_MIN_BATCH_FILES="${BUS_COMPACTED_PARQUET_MIN_BATCH_FILES:-100}" \
  BUS_COMPACTED_PARQUET_MIN_BATCH_BYTES="${BUS_COMPACTED_PARQUET_MIN_BATCH_BYTES:-67108864}" \
  BUS_COMPACTED_PARQUET_MAX_OPEN_LAG_MINUTES="${BUS_COMPACTED_PARQUET_MAX_OPEN_LAG_MINUTES:-60}" \
  BUS_COMPACTED_PARQUET_ALLOW_SMALL_TAIL="${BUS_COMPACTED_PARQUET_ALLOW_SMALL_TAIL:-false}" \
    ./bin/run-raw-parquet-compaction.sh | tee "$batch_log"
  compaction_status=${PIPESTATUS[0]}
  set -e

  if [[ "$compaction_status" -ne 0 ]]; then
    log "compaction batch=$batch failed status=$compaction_status"
    rm -f "$batch_log"
    exit "$compaction_status"
  fi

  compacted=false
  if grep -q "moved .* partitioned parquet files" "$batch_log"; then
    compacted=true
    empty_rounds=0
    log "compaction batch=$batch produced compacted parquet"
  elif grep -Eq "no new parquet files to (process|compact)|no readable new parquet files to compact|waiting for larger fresh batch" "$batch_log"; then
    empty_rounds=$((empty_rounds + 1))
    log "compaction batch=$batch had no ready work empty_rounds=$empty_rounds"
  else
    log "compaction batch=$batch had unknown no-output state; continuing cautiously"
  fi
  rm -f "$batch_log"

  if [[ "$compacted" == "true" && "$cleanup_every_batches" -gt 0 && $((batch % cleanup_every_batches)) -eq 0 ]]; then
    run_cleanup
  elif cleanup_lags_compaction; then
    log "cleanup lags compaction; running cleanup without waiting for a fresh compaction batch"
    run_cleanup
  fi

  if [[ "$compacted" != "true" ]]; then
    log "sleeping after empty batch seconds=$empty_sleep_seconds"
    sleep "$empty_sleep_seconds"
  else
    sleep "$sleep_seconds"
  fi
done
