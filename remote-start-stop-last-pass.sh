#!/bin/bash
set -euo pipefail
cd /home/eljah/apps/buscrawl
pkill -f BusStopLastPassAggregationJob || true
mvn -q -DskipTests compile
setsid -f env BUS_STOP_LAST_PASS_SPARK_MASTER='local[3]' BUS_STOP_LAST_PASS_DRIVER_MEMORY='8g' BUS_STOP_LAST_PASS_EXECUTOR_MEMORY='8g' BUS_STOP_LAST_PASS_OUTPUT_PARTITIONS='64' BUS_STOP_LAST_PASS_INITIAL_LOOKBACK_DAYS='2' BUS_STOP_LAST_PASS_MAX_FILES_PER_RUN='64' ./bin/run-stop-last-pass-aggregation.sh >/home/eljah/apps/buscrawl/logs/stop-last-pass-aggregation.log 2>&1
sleep 2
ps -ef | grep BusStopLastPassAggregationJob | grep -v grep || true
