#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
LOCK_HELD=false
LOCK_LOG=${BUS_HEAVY_JOB_LOCK_LOG:-/home/eljah/apps/buscrawl/logs/heavy-io-lock.log}
lock_log() {
  mkdir -p "$(dirname "$LOCK_LOG")"
  echo "$(date -Is) job=raw-spool-cleanup pid=$$ $*" >> "$LOCK_LOG"
}
release_heavy_lock() {
  local status=$?
  if [[ "$LOCK_HELD" != "true" || "${LOCK_RELEASED:-false}" == "true" ]]; then
    return "$status"
  fi
  LOCK_RELEASED=true
  local released_ms
  released_ms=$(date +%s%3N)
  lock_log "lock=released path=$LOCK_FILE heldMs=$((released_ms - lock_acquired_ms)) status=$status"
  return "$status"
}
trap release_heavy_lock EXIT

if [[ "${BUS_RAW_SPOOL_CLEANUP_SKIP_WHEN_HEAVY_JOB_RUNNING:-true}" == "true" ]]; then
  exec 9>"$LOCK_FILE"
  lock_wait_started_ms=$(date +%s%3N)
  lock_log "lock=try path=$LOCK_FILE"
  if ! flock -n 9; then
    lock_log "lock=busy-skip path=$LOCK_FILE"
    echo "$(date -Is) raw spool cleanup skipped: another heavy derived job is running"
    exit 0
  fi
  LOCK_HELD=true
  lock_acquired_ms=$(date +%s%3N)
  lock_log "lock=acquired path=$LOCK_FILE waitMs=$((lock_acquired_ms - lock_wait_started_ms))"
fi

export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_READY_DIR=${BUS_RAW_SPOOL_READY_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool/ready}
export BUS_STREAM_CHECKPOINT_DIR=${BUS_STREAM_CHECKPOINT_DIR:-${BUS_STORAGE_ROOT}/bus-data-checkpoint}
export BUS_RAW_SPOOL_CLEANUP_STATE_FILE=${BUS_RAW_SPOOL_CLEANUP_STATE_FILE:-${BUS_STORAGE_ROOT}/raw-spool-cleanup-state.json}
export BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS=${BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS:-6}
export BUS_RAW_SPOOL_CLEANUP_MAX_FILES=${BUS_RAW_SPOOL_CLEANUP_MAX_FILES:-500}
export BUS_RAW_SPOOL_CLEANUP_MAX_RECORDS=${BUS_RAW_SPOOL_CLEANUP_MAX_RECORDS:-10000}
export BUS_RAW_SPOOL_CLEANUP_DRY_RUN=${BUS_RAW_SPOOL_CLEANUP_DRY_RUN:-true}

ionice -c3 nice -n 19 /usr/bin/java -cp "target/classes:target/dependency/*" BusRawSpoolCleanupJob
status=$?
release_heavy_lock
exit "$status"
