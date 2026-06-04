#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-${BUS_STORAGE_ROOT}/bus-data-parquet}
export BUS_PARQUET_MIGRATION_OUTPUT_DIR=${BUS_PARQUET_MIGRATION_OUTPUT_DIR:-${BUS_STORAGE_ROOT}/bus-data-parquet-migrated}
export BUS_PARQUET_MIGRATION_SPARK_LOCAL_DIR=${BUS_PARQUET_MIGRATION_SPARK_LOCAL_DIR:-${APP_DIR}/var/bus/parquet-migration-spark-temp}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

cd "${APP_DIR}"
java -cp "target/classes:target/dependency/*" BusParquetTimestampBackfill
