#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TCP_HOST=${BUS_TCP_HOST:-127.0.0.1}
export BUS_TCP_PORT=${BUS_TCP_PORT:-9999}
export BUS_INGEST_SOURCE=${BUS_INGEST_SOURCE:-spool}
export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export BUS_RAW_SPOOL_READY_DIR=${BUS_RAW_SPOOL_READY_DIR:-${BUS_STORAGE_ROOT}/raw-json-spool/ready}
export BUS_RAW_SPOOL_MAX_FILES_PER_TRIGGER=${BUS_RAW_SPOOL_MAX_FILES_PER_TRIGGER:-64}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

# File source checkpoints are durable. Do not delete them during normal restarts.

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusDataSparkStreaming
