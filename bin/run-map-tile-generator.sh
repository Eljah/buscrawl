#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TILE_DIR=${BUS_TILE_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/tiles/base}
export BUS_DASHBOARD_MAP_CONFIG_FILE=${BUS_DASHBOARD_MAP_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/map-config.json}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusOsmTileGenerator
