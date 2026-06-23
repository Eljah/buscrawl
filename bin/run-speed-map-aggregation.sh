#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TRAFFIC_BEHAVIOR_DIR=${BUS_TRAFFIC_BEHAVIOR_DIR:-/home/eljah/data/buscrawl/traffic-behavior}
export BUS_SPEED_MAP_RAW_PARQUET_DIR=${BUS_SPEED_MAP_RAW_PARQUET_DIR:-/home/eljah/data/buscrawl/bus-data-parquet-compacted}
export BUS_SPEED_MAP_DIR=${BUS_SPEED_MAP_DIR:-/home/eljah/data/buscrawl/speed-map}
export BUS_SPEED_MAP_SPARK_LOCAL_DIR=${BUS_SPEED_MAP_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/speed-map-spark-temp}
export BUS_SPEED_MAP_SPARK_MASTER=${BUS_SPEED_MAP_SPARK_MASTER:-local[3]}
export BUS_SPEED_MAP_DRIVER_MEMORY=${BUS_SPEED_MAP_DRIVER_MEMORY:-4g}
export BUS_SPEED_MAP_EXECUTOR_MEMORY=${BUS_SPEED_MAP_EXECUTOR_MEMORY:-4g}
export BUS_SPEED_MAP_OUTPUT_PARTITIONS=${BUS_SPEED_MAP_OUTPUT_PARTITIONS:-24}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusSpeedMapAggregationJob
