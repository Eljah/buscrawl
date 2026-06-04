#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_STOP_LAST_PASS_DIR=${BUS_STOP_LAST_PASS_DIR:-/home/eljah/data/buscrawl/stop-last-pass}
export BUS_DASHBOARD_STOP_LAST_PASS_CACHE_FILE=${BUS_DASHBOARD_STOP_LAST_PASS_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/stop-last-pass.json}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusStopLastPassCacheJob
