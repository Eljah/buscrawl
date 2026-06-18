#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_RAW_DEDUP_INPUT_DIR=${BUS_RAW_DEDUP_INPUT_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_RAW_DEDUP_OUTPUT_DIR=${BUS_RAW_DEDUP_OUTPUT_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-dedup}
export BUS_RAW_DEDUP_TEMP_ROOT=${BUS_RAW_DEDUP_TEMP_ROOT:-/home/eljah/data/buscrawl/bus-data-parquet-dedup-tmp}
export BUS_RAW_DEDUP_SPARK_LOCAL_DIR=${BUS_RAW_DEDUP_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/raw-dedup-spark-temp}
export BUS_RAW_DEDUP_SPARK_MASTER=${BUS_RAW_DEDUP_SPARK_MASTER:-local[2]}
export BUS_RAW_DEDUP_DRIVER_MEMORY=${BUS_RAW_DEDUP_DRIVER_MEMORY:-8g}
export BUS_RAW_DEDUP_EXECUTOR_MEMORY=${BUS_RAW_DEDUP_EXECUTOR_MEMORY:-8g}
export BUS_RAW_DEDUP_OUTPUT_PARTITIONS=${BUS_RAW_DEDUP_OUTPUT_PARTITIONS:-64}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusRawParquetDedupJob
