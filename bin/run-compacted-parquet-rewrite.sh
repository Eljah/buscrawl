#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_COMPACTED_PARQUET_DIR=${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_COMPACTED_REWRITE_STATE_FILE=${BUS_COMPACTED_REWRITE_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/rewrite-state.json}
export BUS_COMPACTED_REWRITE_MIN_FILES=${BUS_COMPACTED_REWRITE_MIN_FILES:-8}
export BUS_COMPACTED_REWRITE_MAX_DATES_PER_RUN=${BUS_COMPACTED_REWRITE_MAX_DATES_PER_RUN:-1}
export BUS_COMPACTED_REWRITE_EXCLUDE_RECENT_DAYS=${BUS_COMPACTED_REWRITE_EXCLUDE_RECENT_DAYS:-2}
export BUS_COMPACTED_REWRITE_MIN_SOURCE_AGE_MINUTES=${BUS_COMPACTED_REWRITE_MIN_SOURCE_AGE_MINUTES:-30}
export BUS_COMPACTED_REWRITE_KEEP_BACKUP=${BUS_COMPACTED_REWRITE_KEEP_BACKUP:-false}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

/usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusCompactedParquetRewriteJob
