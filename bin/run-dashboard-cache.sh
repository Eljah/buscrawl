#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

if [[ "${BUS_SKIP_HEAVY_JOB_LOCK:-false}" != "true" ]]; then
  LOCK_FILE=${BUS_HEAVY_JOB_LOCK_FILE:-/home/eljah/data/buscrawl/derived-jobs.lock}
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "$(date -Is) dashboard cache skipped: another heavy derived job is running"
    exit 0
  fi
fi

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_DASHBOARD_CACHE_FILE=${BUS_DASHBOARD_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/stats.json}
export BUS_DASHBOARD_ROUTE_CACHE_FILE=${BUS_DASHBOARD_ROUTE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/route-last-movement.json}
export BUS_TRAFFIC_BEHAVIOR_DIR=${BUS_TRAFFIC_BEHAVIOR_DIR:-/home/eljah/data/buscrawl/traffic-behavior}
export BUS_DASHBOARD_SPARK_LOCAL_DIR=${BUS_DASHBOARD_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/dashboard-spark-temp}
export BUS_DASHBOARD_SPARK_MASTER=${BUS_DASHBOARD_SPARK_MASTER:-local[1]}
export BUS_DASHBOARD_SPARK_DRIVER_MEMORY=${BUS_DASHBOARD_SPARK_DRIVER_MEMORY:-3g}
export BUS_DASHBOARD_SPARK_EXECUTOR_MEMORY=${BUS_DASHBOARD_SPARK_EXECUTOR_MEMORY:-3g}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusDashboardCacheJob
