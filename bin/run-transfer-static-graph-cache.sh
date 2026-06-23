#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TRANSFER_STATIC_GRAPH_DIR=${BUS_TRANSFER_STATIC_GRAPH_DIR:-/home/eljah/data/buscrawl/transfer-static-graph}
export BUS_TRANSFER_STATIC_MAX_TRANSFERS=${BUS_TRANSFER_STATIC_MAX_TRANSFERS:-3}
export BUS_TRANSFER_STATIC_WALK_RADIUS_METERS=${BUS_TRANSFER_STATIC_WALK_RADIUS_METERS:-300}
export BUS_TRANSFER_STATIC_MAX_SETTLED_STATES_PER_ORIGIN=${BUS_TRANSFER_STATIC_MAX_SETTLED_STATES_PER_ORIGIN:-250000}

exec /usr/bin/java \
  -Xmx8g \
  -cp "target/classes:target/dependency/*" \
  BusTransferStaticGraphCacheJob
