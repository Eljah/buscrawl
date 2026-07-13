#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
LOCK_LOG=${BUS_HEAVY_JOB_LOCK_LOG:-/home/eljah/apps/buscrawl/logs/heavy-io-lock.log}
lock_log() {
  mkdir -p "$(dirname "$LOCK_LOG")"
  echo "$(date -Is) job=raw-parquet-manifest-bootstrap pid=$$ $*" >> "$LOCK_LOG"
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

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_RAW_PARQUET_MANIFEST_FILE=${BUS_RAW_PARQUET_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

ionice -c3 nice -n 19 /usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetManifestBootstrapJob
status=$?
release_heavy_lock
exit "$status"
