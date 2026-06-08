#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TCP_PORT=${BUS_TCP_PORT:-9999}
export BUS_TCP_ENABLED=${BUS_TCP_ENABLED:-false}
export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_DIR=${BUS_RAW_SPOOL_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool}
export BUS_VERBOSE_LOG=${BUS_VERBOSE_LOG:-false}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusRealtimeClient
