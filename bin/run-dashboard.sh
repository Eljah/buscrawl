#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_DASHBOARD_CACHE_FILE=${BUS_DASHBOARD_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/stats.json}
export BUS_DASHBOARD_ROUTE_CACHE_FILE=${BUS_DASHBOARD_ROUTE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/route-last-movement.json}
export BUS_DASHBOARD_TRACE_CACHE_FILE=${BUS_DASHBOARD_TRACE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/bus-traces.json}
export BUS_DASHBOARD_MAP_CONFIG_FILE=${BUS_DASHBOARD_MAP_CONFIG_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/map-config.json}
export BUS_TILE_DIR=${BUS_TILE_DIR:-/home/eljah/apps/buscrawl/dashboard-cache/tiles/base}
export BUS_DASHBOARD_PORT=${BUS_DASHBOARD_PORT:-8061}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusDashboardServer
