#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_DIR=${BUS_RAW_SPOOL_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool}
export BUS_NAVI_ROUTE_MAP_FILE=${BUS_NAVI_ROUTE_MAP_FILE:-/home/eljah/apps/buscrawl/src/main/resources/navi-route-map.json}
export BUS_NAVI_POLL_SECONDS=${BUS_NAVI_POLL_SECONDS:-30}
export BUS_NAVI_POLL_JITTER_SECONDS=${BUS_NAVI_POLL_JITTER_SECONDS:-5}
export BUS_NAVI_FAILURE_BACKOFF_BASE_SECONDS=${BUS_NAVI_FAILURE_BACKOFF_BASE_SECONDS:-30}
export BUS_NAVI_FAILURE_BACKOFF_MAX_SECONDS=${BUS_NAVI_FAILURE_BACKOFF_MAX_SECONDS:-300}
export BUS_NAVI_MIN_LAT=${BUS_NAVI_MIN_LAT:-55.55}
export BUS_NAVI_MAX_LAT=${BUS_NAVI_MAX_LAT:-56.05}
export BUS_NAVI_MIN_LON=${BUS_NAVI_MIN_LON:-48.75}
export BUS_NAVI_MAX_LON=${BUS_NAVI_MAX_LON:-49.55}
export BUS_NAVI_MAX_SOURCE_LAG_SECONDS=${BUS_NAVI_MAX_SOURCE_LAG_SECONDS:-900}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusNaviRealtimeClient
