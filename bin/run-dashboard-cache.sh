#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_DASHBOARD_CACHE_FILE=${BUS_DASHBOARD_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/stats.json}
export BUS_DASHBOARD_ROUTE_CACHE_FILE=${BUS_DASHBOARD_ROUTE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/route-last-movement.json}
export BUS_DASHBOARD_SPARK_LOCAL_DIR=${BUS_DASHBOARD_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/dashboard-spark-temp}
export BUS_DASHBOARD_SPARK_MASTER=${BUS_DASHBOARD_SPARK_MASTER:-local[2]}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusDashboardCacheJob
