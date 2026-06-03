#!/bin/bash
set -euo pipefail

cd /home/eljah/apps/buscrawl

export BUS_TCP_HOST=${BUS_TCP_HOST:-127.0.0.1}
export BUS_TCP_PORT=${BUS_TCP_PORT:-9999}
export BUS_STORAGE_ROOT=${BUS_STORAGE_ROOT:-/home/eljah/data/buscrawl}
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost
export JAVA_TOOL_OPTIONS='--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED'

# Socket source checkpoints are not restart-safe in this setup.
rm -rf "${BUS_STORAGE_ROOT}/bus-data-checkpoint"

exec /usr/bin/java \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -cp "target/classes:target/dependency/*" \
  BusDataSparkStreaming
