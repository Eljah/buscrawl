#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_COMPACTED_DEDUP_INPUT_DIR=${BUS_COMPACTED_DEDUP_INPUT_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_COMPACTED_DEDUP_OUTPUT_DIR=${BUS_COMPACTED_DEDUP_OUTPUT_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted-dedup}
export BUS_COMPACTED_DEDUP_TEMP_ROOT=${BUS_COMPACTED_DEDUP_TEMP_ROOT:-/home/eljah/data/buscrawl/bus-data-parquet-compacted-dedup-tmp}
export BUS_COMPACTED_DEDUP_THREADS=${BUS_COMPACTED_DEDUP_THREADS:-2}
export BUS_COMPACTED_DEDUP_MEMORY_LIMIT=${BUS_COMPACTED_DEDUP_MEMORY_LIMIT:-8GB}

exec /usr/bin/java \
  -Xmx1g \
  -cp "target/classes:target/dependency/*" \
  BusCompactedParquetDedupDuckDbJob
