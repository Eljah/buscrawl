#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_SPEED_MAP_DIR=${BUS_SPEED_MAP_DIR:-/home/eljah/data/buscrawl/speed-map}
export BUS_DASHBOARD_SPEED_MAP_CACHE_FILE=${BUS_DASHBOARD_SPEED_MAP_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/speed-map.json}
export BUS_DASHBOARD_MAP_CONFIG_FILE=${BUS_DASHBOARD_MAP_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/map-config.json}
export BUS_TILE_ROOT=${BUS_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles}
export BUS_SPEED_MAP_CACHE_ROW_LIMIT=${BUS_SPEED_MAP_CACHE_ROW_LIMIT:-500}
export BUS_SPEED_MAP_POINT_CACHE_ROW_LIMIT=${BUS_SPEED_MAP_POINT_CACHE_ROW_LIMIT:-250}
export BUS_SPEED_MAP_RENDER_TILES=${BUS_SPEED_MAP_RENDER_TILES:-true}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusSpeedMapCacheJob
