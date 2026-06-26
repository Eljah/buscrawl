#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TRAFFIC_BEHAVIOR_DIR=${BUS_TRAFFIC_BEHAVIOR_DIR:-/home/eljah/data/buscrawl/traffic-behavior}
export BUS_ACCESSIBILITY_SOURCE=${BUS_ACCESSIBILITY_SOURCE:-transfer-potential}
export BUS_ACCESSIBILITY_OSM_ROADS_FILE=${BUS_ACCESSIBILITY_OSM_ROADS_FILE:-/home/eljah/apps/buscrawl/osm-cache/overpass-kazan-roads.json}
export BUS_ACCESSIBILITY_MAP_CACHE_FILE=${BUS_ACCESSIBILITY_MAP_CACHE_FILE:-/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map.json}
export BUS_ACCESSIBILITY_TILE_ROOT=${BUS_ACCESSIBILITY_TILE_ROOT:-/home/eljah/apps/buscrawl/dashboard-cache/tiles/accessibility/current}
export BUS_ACCESSIBILITY_ORIGIN_STOP=${BUS_ACCESSIBILITY_ORIGIN_STOP:-Tasma}
export BUS_ACCESSIBILITY_ORIGIN_STOP_IDS=${BUS_ACCESSIBILITY_ORIGIN_STOP_IDS:-12078,12112}
export BUS_ACCESSIBILITY_DEPARTURE_START=${BUS_ACCESSIBILITY_DEPARTURE_START:-04:00}
export BUS_ACCESSIBILITY_DEPARTURE_END=${BUS_ACCESSIBILITY_DEPARTURE_END:-23:45}
export BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES=${BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES:-15}
export BUS_ACCESSIBILITY_RENDER_MODES=${BUS_ACCESSIBILITY_RENDER_MODES:-total,totalFull,totalLog,walk,stopTransport}
export BUS_ACCESSIBILITY_SPARK_LOCAL_DIR=${BUS_ACCESSIBILITY_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/accessibility-map-spark-temp}
export BUS_ACCESSIBILITY_SPARK_MASTER=${BUS_ACCESSIBILITY_SPARK_MASTER:-local[2]}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec ionice -c2 -n7 nice -n 10 /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusAccessibilityMapCacheJob
