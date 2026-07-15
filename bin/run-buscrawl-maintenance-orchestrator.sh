#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$APP_DIR"

LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
LOCK_LOG=${BUS_HEAVY_JOB_LOCK_LOG:-/home/eljah/apps/buscrawl/logs/heavy-io-lock.log}

lock_log() {
  mkdir -p "$(dirname "$LOCK_LOG")"
  echo "$(date -Is) job=buscrawl-maintenance pid=$$ $*" >> "$LOCK_LOG"
}

exec 9>"$LOCK_FILE"
lock_wait_started_ms=$(date +%s%3N)
lock_log "lock=wait path=$LOCK_FILE"
flock 9
lock_acquired_ms=$(date +%s%3N)
lock_log "lock=acquired path=$LOCK_FILE waitMs=$((lock_acquired_ms - lock_wait_started_ms))"

release_heavy_lock() {
  local status=$?
  if [[ "${LOCK_RELEASED:-false}" == "true" ]]; then
    return "$status"
  fi
  LOCK_RELEASED=true
  local released_ms
  released_ms=$(date +%s%3N)
  lock_log "lock=released path=$LOCK_FILE heldMs=$((released_ms - lock_acquired_ms)) status=$status"
  return "$status"
}
trap release_heavy_lock EXIT

echo "$(date -Is) buscrawl maintenance orchestrator started"

compaction_max_batches=${BUS_MAINTENANCE_COMPACTION_MAX_BATCHES:-${BUS_DERIVED_COMPACTION_MAX_BATCHES:-8}}
compaction_max_files_per_run=${BUS_MAINTENANCE_COMPACTION_MAX_FILES_PER_RUN:-${BUS_DERIVED_COMPACTION_MAX_FILES_PER_RUN:-20000}}
idle_cleanup_max_cycles=${BUS_MAINTENANCE_IDLE_CLEANUP_MAX_CYCLES:-6}
compacted_any=false

run_compaction_batch() {
  local batch="$1"
  local batch_log="$2"
  BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN="$compaction_max_files_per_run" \
  BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS="${BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS:-8}" \
  BUS_COMPACTED_PARQUET_MIN_BATCH_FILES="${BUS_COMPACTED_PARQUET_MIN_BATCH_FILES:-100}" \
  BUS_COMPACTED_PARQUET_MIN_BATCH_BYTES="${BUS_COMPACTED_PARQUET_MIN_BATCH_BYTES:-67108864}" \
  BUS_COMPACTED_PARQUET_MAX_OPEN_LAG_MINUTES="${BUS_COMPACTED_PARQUET_MAX_OPEN_LAG_MINUTES:-60}" \
  BUS_COMPACTED_PARQUET_ALLOW_SMALL_TAIL="${BUS_COMPACTED_PARQUET_ALLOW_SMALL_TAIL:-false}" \
  BUS_SKIP_HEAVY_JOB_LOCK=true \
    ./bin/run-raw-parquet-compaction.sh | tee "$batch_log"

  if grep -q "moved .* partitioned parquet files" "$batch_log"; then
    compacted_any=true
    echo "$(date -Is) maintenance raw parquet compaction batch $batch completed"
    return 0
  fi
  if grep -q "waiting for larger fresh batch" "$batch_log"; then
    return 10
  fi
  if grep -Eq "no new parquet files to (process|compact)" "$batch_log"; then
    return 11
  fi
  if grep -q "no readable new parquet files to compact" "$batch_log"; then
    return 12
  fi
  return 13
}

run_cleanup_quantum() {
  local cleanup_log
  cleanup_log="$(mktemp)"
  if [[ "${BUS_MAINTENANCE_RUN_RAW_PARQUET_CLEANUP:-true}" == "true" ]]; then
    if [[ -x ./bin/run-raw-parquet-cleanup.sh ]]; then
      BUS_RAW_PARQUET_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
        BUS_RAW_PARQUET_CLEANUP_DRY_RUN="${BUS_RAW_PARQUET_CLEANUP_DRY_RUN:-false}" \
        BUS_RAW_PARQUET_CLEANUP_MAX_FILES="${BUS_RAW_PARQUET_CLEANUP_MAX_FILES:-5000}" \
        BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS="${BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS:-10000}" \
        ./bin/run-raw-parquet-cleanup.sh | tee "$cleanup_log"
    else
      echo "$(date -Is) raw parquet cleanup script is absent; skipping"
    fi
  fi

  if [[ "${BUS_MAINTENANCE_RUN_RAW_SPOOL_CLEANUP:-true}" == "true" ]]; then
    BUS_RAW_SPOOL_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
      BUS_RAW_SPOOL_CLEANUP_DRY_RUN="${BUS_RAW_SPOOL_CLEANUP_DRY_RUN:-false}" \
      ./bin/run-raw-spool-cleanup.sh
  fi

  if grep -q "stopReason=end" "$cleanup_log" || grep -q "stopReason=minAge" "$cleanup_log"; then
    rm -f "$cleanup_log"
    return 1
  fi
  rm -f "$cleanup_log"
  return 0
}

for batch in $(seq 1 "$compaction_max_batches"); do
  batch_log="$(mktemp)"
  if run_compaction_batch "$batch" "$batch_log"; then
    compaction_status=0
  else
    compaction_status=$?
  fi
  rm -f "$batch_log"
  if [[ "$compaction_status" -ne 0 ]]; then
    break
  fi
done

if [[ "$compacted_any" != "true" ]]; then
  echo "$(date -Is) no ready compaction batch; entering bounded cleanup-while-waiting mode cycles=$idle_cleanup_max_cycles"
  for cycle in $(seq 1 "$idle_cleanup_max_cycles"); do
    if ! run_cleanup_quantum; then
      echo "$(date -Is) cleanup quantum found no more old raw work; leaving idle cleanup mode"
      break
    fi
    batch_log="$(mktemp)"
    if run_compaction_batch "idle-$cycle" "$batch_log"; then
      compaction_status=0
    else
      compaction_status=$?
    fi
    rm -f "$batch_log"
    if [[ "$compaction_status" -eq 0 ]]; then
      echo "$(date -Is) fresh compaction batch became ready during cleanup; leaving idle cleanup mode"
      break
    fi
    if [[ "$compaction_status" -ne 10 && "$compaction_status" -ne 11 ]]; then
      echo "$(date -Is) compaction returned status=$compaction_status during idle cleanup"
      break
    fi
  done
fi

if [[ "$compacted_any" == "true" ]]; then
  if [[ "${BUS_MAINTENANCE_RUN_RAW_PARQUET_CLEANUP:-true}" == "true" ]]; then
    if [[ -x ./bin/run-raw-parquet-cleanup.sh ]]; then
      BUS_RAW_PARQUET_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
        BUS_RAW_PARQUET_CLEANUP_DRY_RUN="${BUS_RAW_PARQUET_CLEANUP_DRY_RUN:-false}" \
        ./bin/run-raw-parquet-cleanup.sh
    else
      echo "$(date -Is) raw parquet cleanup script is absent; skipping"
    fi
  fi

  if [[ "${BUS_MAINTENANCE_RUN_RAW_SPOOL_CLEANUP:-true}" == "true" ]]; then
    BUS_RAW_SPOOL_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING=false \
      BUS_RAW_SPOOL_CLEANUP_DRY_RUN="${BUS_RAW_SPOOL_CLEANUP_DRY_RUN:-false}" \
      ./bin/run-raw-spool-cleanup.sh
  fi
fi

if [[ "$compacted_any" != "true" && "${BUS_MAINTENANCE_RUN_DERIVED_WITHOUT_NEW_COMPACTED:-false}" != "true" ]]; then
  echo "$(date -Is) no new compacted batch; skipping downstream derived/cache in this maintenance pass"
  echo "$(date -Is) buscrawl maintenance orchestrator finished"
  exit 0
fi

BUS_DERIVED_ASSUME_HEAVY_JOB_LOCK=true \
BUS_DERIVED_SKIP_RAW_COMPACTION=true \
BUS_DERIVED_RUN_RAW_CLEANUP_AFTER_COMPACTION=false \
  ./bin/run-derived-data-catchup.sh

echo "$(date -Is) buscrawl maintenance orchestrator finished"
