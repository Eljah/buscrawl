#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TRAFFIC_BEHAVIOR_DIR=${BUS_TRAFFIC_BEHAVIOR_DIR:-/home/eljah/data/buscrawl/traffic-behavior}
export BUS_DASHBOARD_RUBBERINESS_CACHE_FILE=${BUS_DASHBOARD_RUBBERINESS_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/rubberiness.json}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusRubberinessCacheJob
