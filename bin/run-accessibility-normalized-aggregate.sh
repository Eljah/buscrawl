#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_DASHBOARD_ACCESSIBILITY_MAP_DIR=${BUS_DASHBOARD_ACCESSIBILITY_MAP_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-origins-render-cache}
export BUS_ACCESSIBILITY_AGGREGATE_ORIGIN_DIR=${BUS_ACCESSIBILITY_AGGREGATE_ORIGIN_DIR:-$BUS_DASHBOARD_ACCESSIBILITY_MAP_DIR}
export BUS_ACCESSIBILITY_AGGREGATE_OUTPUT_DIR=${BUS_ACCESSIBILITY_AGGREGATE_OUTPUT_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-aggregates}
export BUS_TILE_ROOT=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
export BUS_ACCESSIBILITY_AGGREGATE_TILE_PREFIX=${BUS_ACCESSIBILITY_AGGREGATE_TILE_PREFIX:-accessibility-aggregates}
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec ionice -c2 -n7 nice -n 10 /usr/bin/java \
  -cp "target/classes:target/dependency/*" \
  BusAccessibilityNormalizedAggregateJob
