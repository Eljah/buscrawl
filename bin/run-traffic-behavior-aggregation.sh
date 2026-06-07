#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_STOP_VISITS_DIR=${BUS_STOP_VISITS_DIR:-/home/eljah/data/buscrawl/stop-last-pass/stop-visit-events}
export BUS_TRAFFIC_BEHAVIOR_DIR=${BUS_TRAFFIC_BEHAVIOR_DIR:-/home/eljah/data/buscrawl/traffic-behavior}
export BUS_TRAFFIC_BEHAVIOR_STATE_FILE=${BUS_TRAFFIC_BEHAVIOR_STATE_FILE:-/home/eljah/data/buscrawl/traffic-behavior/aggregation-state.json}
export BUS_TRAFFIC_BEHAVIOR_SPARK_LOCAL_DIR=${BUS_TRAFFIC_BEHAVIOR_SPARK_LOCAL_DIR:-/home/eljah/data/buscrawl/traffic-behavior-spark-temp}
export BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER=${BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER:-local[2]}
export BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY=${BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY:-4g}
export BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY=${BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY:-4g}
export BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS=${BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS:-32}
export BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN=${BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN:-512}
export BUS_TRAFFIC_BEHAVIOR_INITIAL_LOOKBACK_DAYS=${BUS_TRAFFIC_BEHAVIOR_INITIAL_LOOKBACK_DAYS:-3}
export BUS_TRAFFIC_BEHAVIOR_BOOTSTRAP_MODE=${BUS_TRAFFIC_BEHAVIOR_BOOTSTRAP_MODE:-full-history}
export BUS_SEGMENT_MAX_GAP_SECONDS=${BUS_SEGMENT_MAX_GAP_SECONDS:-900}
export BUS_SEGMENT_MIN_SPEED_KMH=${BUS_SEGMENT_MIN_SPEED_KMH:-3.0}
export BUS_SEGMENT_MAX_SPEED_KMH=${BUS_SEGMENT_MAX_SPEED_KMH:-80.0}
export BUS_CITY_TIMEZONE=${BUS_CITY_TIMEZONE:-Europe/Moscow}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusTrafficBehaviorAggregationJob
