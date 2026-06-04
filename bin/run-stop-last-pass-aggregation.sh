#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_STOP_LAST_PASS_DIR=${BUS_STOP_LAST_PASS_DIR:-/home/eljah/data/buscrawl/stop-last-pass}
export BUS_STOP_LAST_PASS_STATE_FILE=${BUS_STOP_LAST_PASS_STATE_FILE:-/home/eljah/data/buscrawl/stop-last-pass/aggregation-state.json}
export BUS_STOP_LAST_PASS_SPARK_LOCAL_DIR=${BUS_STOP_LAST_PASS_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/stop-last-pass-spark-temp}
export BUS_STOP_LAST_PASS_SPARK_MASTER=${BUS_STOP_LAST_PASS_SPARK_MASTER:-local[2]}
export BUS_STOP_VISIT_MAX_GAP_SECONDS=${BUS_STOP_VISIT_MAX_GAP_SECONDS:-180}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusStopLastPassAggregationJob
