#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_COMPACTED_PARQUET_DIR=${BUS_COMPACTED_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_COMPACTED_PARQUET_STATE_FILE=${BUS_COMPACTED_PARQUET_STATE_FILE:-/home/eljah/data/buscrawl/bus-data-parquet-compacted/compaction-state.json}
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

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetCompactionJob
