#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_DASHBOARD_TRACE_CACHE_FILE=${BUS_DASHBOARD_TRACE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/bus-traces.json}
export BUS_DASHBOARD_MAP_CONFIG_FILE=${BUS_DASHBOARD_MAP_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/map-config.json}
export BUS_TILE_ROOT=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
export BUS_TRACE_SPARK_MASTER=${BUS_TRACE_SPARK_MASTER:-local[2]}
export BUS_TRACE_SPARK_LOCAL_DIR=${BUS_TRACE_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/trace-cache-spark-temp}
export BUS_TRACE_SPARK_DRIVER_MEMORY=${BUS_TRACE_SPARK_DRIVER_MEMORY:-2g}
export BUS_TRACE_SPARK_EXECUTOR_MEMORY=${BUS_TRACE_SPARK_EXECUTOR_MEMORY:-2g}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusActiveTraceCacheJob
