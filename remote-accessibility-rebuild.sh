#!/bin/bash
set -euo pipefail
cd /home/eljah/apps/buscrawl
mkdir -p logs
LOG=logs/accessibility-rebuild-$(date +%Y%m%d-%H%M%S).log
exec >> "$LOG" 2>&1

echo "$(date -Is) accessibility rebuild started"

echo "$(date -Is) backing up traffic state"
if [ -f /home/eljah/data/buscrawl/traffic-behavior/aggregation-state.json ]; then
  cp /home/eljah/data/buscrawl/traffic-behavior/aggregation-state.json "/home/eljah/data/buscrawl/traffic-behavior/aggregation-state.before-terminal-fix.$(date +%Y%m%d-%H%M%S).json"
fi
rm -f /home/eljah/data/buscrawl/traffic-behavior/aggregation-state.json

echo "$(date -Is) rebuilding traffic behavior from all stop visits"
BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN=100000 \
BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER=local[2] \
BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY=8g \
BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY=8g \
BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS=16 \
BUS_TRAFFIC_BEHAVIOR_EVENTS_ONLY=true \
./bin/run-traffic-behavior-aggregation.sh

echo "$(date -Is) validating restored target terminal segment trips"
cat > /tmp/ValidateTerminalTargets.java <<'JAVA'
import java.sql.*;
public class ValidateTerminalTargets {
  public static void main(String[] args) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement s=c.createStatement()) { s.execute("INSTALL parquet"); s.execute("LOAD parquet"); }
      String base="/home/eljah/data/buscrawl/traffic-behavior/segment-trips/serviceDate=2026-06-21/*.parquet";
      String q="select endStopId,endStopName,routeNumber,count(*) cnt,min(endEnteredStopAt),max(endEnteredStopAt) from read_parquet('"+base+"') where endStopId in ('11719','176892','11748','11689','11969','11985','11986','109495','109499') group by 1,2,3 order by endStopId,routeNumber";
      try (ResultSet rs=c.createStatement().executeQuery(q)) {
        while(rs.next()) System.out.println(rs.getString(1)+" "+rs.getString(2)+" route="+rs.getString(3)+" cnt="+rs.getLong(4)+" first="+rs.getTimestamp(5)+" last="+rs.getTimestamp(6));
      }
    }
  }
}
JAVA
javac -encoding UTF-8 -cp 'target/dependency/*' /tmp/ValidateTerminalTargets.java
java -cp '/tmp:target/dependency/*' ValidateTerminalTargets

echo "$(date -Is) preparing transfer state for forced 2026-06-21 rebuild"
python3 - <<'PY'
import json, os, shutil, time
p='/home/eljah/data/buscrawl/transfer-potential/aggregation-state.json'
if os.path.exists(p):
    shutil.copy2(p, p + '.before-terminal-fix.' + time.strftime('%Y%m%d-%H%M%S'))
    state=json.load(open(p,encoding='utf-8'))
else:
    state={}
keys=[k for k in state.get('processedBucketKeys',[]) if not str(k).startswith('2026-06-21|')]
state['processedBucketKeys']=keys
state['processedServiceDates']=[d for d in state.get('processedServiceDates',[]) if d!='2026-06-21']
state['updatedAt']='2026-06-22T00:00:00Z'
json.dump(state, open(p,'w',encoding='utf-8'), ensure_ascii=False, indent=2)
PY

echo "$(date -Is) rebuilding transfer potential for 2026-06-21"
BUS_TRANSFER_TARGET_DATE=2026-06-21 \
BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME=23:59 \
BUS_TRANSFER_MAX_BUCKETS_PER_RUN=100000 \
BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_ROUTE_PATTERN=6 \
BUS_TRANSFER_POTENTIAL_SPARK_MASTER=local[2] \
BUS_TRANSFER_POTENTIAL_DRIVER_MEMORY=8g \
BUS_TRANSFER_POTENTIAL_EXECUTOR_MEMORY=8g \
./bin/run-transfer-potential.sh

echo "$(date -Is) rebuilding accessibility map cache"
BUS_ACCESSIBILITY_SOURCE=transfer-potential \
BUS_TRANSFER_POTENTIAL_DIR=/home/eljah/data/buscrawl/transfer-potential \
BUS_ACCESSIBILITY_ORIGIN_STOP_IDS=12078,12112 \
BUS_ACCESSIBILITY_ORIGIN_STOP=Tasma \
BUS_ACCESSIBILITY_SERVICE_DATE=2026-06-21 \
BUS_ACCESSIBILITY_DEPARTURE_TIME=08:00 \
./bin/run-accessibility-map-cache.sh

echo "$(date -Is) validating accessibility transfer targets"
cat > /tmp/ValidateTransferTargets.java <<'JAVA'
import java.sql.*;
public class ValidateTransferTargets {
  public static void main(String[] args) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement s=c.createStatement()) { s.execute("INSTALL parquet"); s.execute("LOAD parquet"); }
      String base="/home/eljah/data/buscrawl/transfer-potential/journeys/serviceDate=2026-06-21/departureBucketMinute=480/*.parquet";
      String q="select originStopId,originStopName,destinationStopId,destinationStopName,min(totalJourneySeconds),count(*) from read_parquet('"+base+"') where originStopId in ('12078','12112') and (destinationStopId in ('11719','176892','11748','11689','11969','11985','11986','109495','109499') or destinationStopName like '%Привокз%' or destinationStopName like '%Мосто%') group by 1,2,3,4 order by 4";
      try (ResultSet rs=c.createStatement().executeQuery(q)) {
        while(rs.next()) System.out.println(rs.getString(1)+" "+rs.getString(2)+" -> "+rs.getString(3)+" "+rs.getString(4)+" sec="+rs.getInt(5)+" rows="+rs.getLong(6));
      }
    }
  }
}
JAVA
javac -encoding UTF-8 -cp 'target/dependency/*' /tmp/ValidateTransferTargets.java
java -cp '/tmp:target/dependency/*' ValidateTransferTargets

echo "$(date -Is) accessibility rebuild finished"
