#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TCP_PORT=${BUS_TCP_PORT:-9999}
export BUS_VERBOSE_LOG=${BUS_VERBOSE_LOG:-false}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusRealtimeClient
