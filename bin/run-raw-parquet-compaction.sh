#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOCK_HELD=false
LOCK_LOG=${BUS_HEAVY_JOB_LOCK_LOG:-/home/eljah/apps/buscrawl/logs/heavy-io-lock.log}
lock_log() {
  mkdir -p "$(dirname "$LOCK_LOG")"
  echo "$(date -Is) job=raw-parquet-compaction pid=$$ $*" >> "$LOCK_LOG"
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

if [[ "${BUS_SKIP_HEAVY_JOB_LOCK:-false}" != "true" ]]; then
  LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
  exec 9>"$LOCK_FILE"
  lock_wait_started_ms=$(date +%s%3N)
  lock_log "lock=try path=$LOCK_FILE"
  if ! flock -n 9; then
    lock_log "lock=busy-skip path=$LOCK_FILE"
    echo "$(date -Is) raw parquet compaction skipped: another heavy derived job is running"
    exit 0
  fi
  LOCK_HELD=true
  lock_acquired_ms=$(date +%s%3N)
  lock_log "lock=acquired path=$LOCK_FILE waitMs=$((lock_acquired_ms - lock_wait_started_ms))"
fi

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_COMPACTED_PARQUET_DIR=${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_COMPACTED_PARQUET_STATE_FILE=${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}
export BUS_RAW_PARQUET_MANIFEST_FILE=${BUS_RAW_PARQUET_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-manifest.tsv}
export BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE=${BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv}
export BUS_COMPACTED_PARQUET_USE_MANIFEST=${BUS_COMPACTED_PARQUET_USE_MANIFEST:-true}
export BUS_COMPACTED_PARQUET_SPARK_LOCAL_DIR=${BUS_COMPACTED_PARQUET_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/compaction-spark-temp}
export BUS_COMPACTED_PARQUET_SPARK_MASTER=${BUS_COMPACTED_PARQUET_SPARK_MASTER:-local[2]}
export BUS_COMPACTED_PARQUET_DRIVER_MEMORY=${BUS_COMPACTED_PARQUET_DRIVER_MEMORY:-4g}
export BUS_COMPACTED_PARQUET_EXECUTOR_MEMORY=${BUS_COMPACTED_PARQUET_EXECUTOR_MEMORY:-4g}
export BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS=${BUS_COMPACTED_PARQUET_OUTPUT_PARTITIONS:-16}
export BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN=${BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN:-20000}
export BUS_COMPACTED_PARQUET_INITIAL_LOOKBACK_DAYS=${BUS_COMPACTED_PARQUET_INITIAL_LOOKBACK_DAYS:-3}
export BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE=${BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE:-full-history}
export BUS_COMPACTED_PARQUET_NEWEST_FIRST=${BUS_COMPACTED_PARQUET_NEWEST_FIRST:-false}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

/usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetCompactionJob
status=$?
release_heavy_lock
exit "$status"
