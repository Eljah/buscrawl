#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
exec 9>"$LOCK_FILE"
flock 9

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_RAW_PARQUET_MANIFEST_FILE=${BUS_RAW_PARQUET_MANIFEST_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-legacy-manifest.tsv}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec ionice -c3 nice -n 19 /usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetManifestBootstrapJob
