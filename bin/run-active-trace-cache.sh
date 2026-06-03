#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_PARQUET_DIR=${BUS_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet}
export BUS_DASHBOARD_TRACE_CACHE_FILE=${BUS_DASHBOARD_TRACE_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/bus-traces.json}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusActiveTraceCacheJob
