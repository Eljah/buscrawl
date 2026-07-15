#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_COMPACTED_PARQUET_STATE_FILE=${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}
export BUS_RAW_PARQUET_MANIFEST_FILE=${BUS_RAW_PARQUET_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-manifest.tsv}
export BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE=${BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv}
export BUS_RAW_PARQUET_CLEANUP_STATE_FILE=${BUS_RAW_PARQUET_CLEANUP_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-cleanup-state.json}
export BUS_RAW_PARQUET_CLEANUP_MAX_FILES=${BUS_RAW_PARQUET_CLEANUP_MAX_FILES:-1000}
export BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS=${BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS:-2000}
export BUS_RAW_PARQUET_CLEANUP_MIN_AGE_HOURS=${BUS_RAW_PARQUET_CLEANUP_MIN_AGE_HOURS:-1}
export BUS_RAW_PARQUET_CLEANUP_DRY_RUN=${BUS_RAW_PARQUET_CLEANUP_DRY_RUN:-true}

exec ionice -c3 nice -n 19 /usr/bin/java -cp "target/classes:target/dependency/*" BusRawParquetCleanupJob
