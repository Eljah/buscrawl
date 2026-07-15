#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_READY_DIR=${BUS_RAW_SPOOL_READY_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool/ready}
export BUS_STREAM_CHECKPOINT_DIR=${BUS_STREAM_CHECKPOINT_DIR:-${BUS_STORAGE_ROOT}/bus-data-checkpoint}
export BUS_RAW_SPOOL_CLEANUP_STATE_FILE=${BUS_RAW_SPOOL_CLEANUP_STATE_FILE:-${BUS_STORAGE_ROOT}/raw-spool-cleanup-state.json}
export BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS=${BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS:-6}
export BUS_RAW_SPOOL_CLEANUP_MAX_FILES=${BUS_RAW_SPOOL_CLEANUP_MAX_FILES:-500}
export BUS_RAW_SPOOL_CLEANUP_MAX_RECORDS=${BUS_RAW_SPOOL_CLEANUP_MAX_RECORDS:-10000}
export BUS_RAW_SPOOL_CLEANUP_DRY_RUN=${BUS_RAW_SPOOL_CLEANUP_DRY_RUN:-true}

exec ionice -c3 nice -n 19 /usr/bin/java -cp "target/classes:target/dependency/*" BusRawSpoolCleanupJob
