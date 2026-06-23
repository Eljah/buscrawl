#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_SPEED_MAP_RAW_PARQUET_DIR=${BUS_SPEED_MAP_RAW_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_SPEED_MAP_DIR=${BUS_SPEED_MAP_DIR:-/home/eljah/data/buscrawl/speed-map}
export BUS_SPEED_MAP_MIN_ZOOM=${BUS_SPEED_MAP_MIN_ZOOM:-11}
export BUS_SPEED_MAP_MAX_ZOOM=${BUS_SPEED_MAP_MAX_ZOOM:-15}
export BUS_SPEED_MAP_HEATMAP_BUCKET_PIXELS=${BUS_SPEED_MAP_HEATMAP_BUCKET_PIXELS:-6}
export BUS_SPEED_MAP_DUCKDB_THREADS=${BUS_SPEED_MAP_DUCKDB_THREADS:-4}

exec /usr/bin/java -cp "target/classes:target/dependency/*" BusSpeedCoordinateBucketJob
