#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_DASHBOARD_CACHE_FILE=${BUS_DASHBOARD_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/stats.json}
export BUS_DASHBOARD_ROUTE_CACHE_FILE=${BUS_DASHBOARD_ROUTE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/route-last-movement.json}
export BUS_DASHBOARD_PORT=${BUS_DASHBOARD_PORT:-8061}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusDashboardServer
