#!/bin/bash
set -euo pipefail
cd /home/eljah/apps/buscrawl
mkdir -p logs
LOG=logs/transfer-480-accessibility-$(date +%Y%m%d-%H%M%S).log
exec >> "$LOG" 2>&1

echo "$(date -Is) transfer bucket 480 rebuild started"
python3 - <<'PY'
import json, os, shutil, time
p='/home/eljah/data/buscrawl/transfer-potential/aggregation-state.json'
if os.path.exists(p):
    shutil.copy2(p, p + '.before-bucket-480-rebuild.' + time.strftime('%Y%m%d-%H%M%S'))
    state=json.load(open(p,encoding='utf-8'))
else:
    state={}
state['processedBucketKeys']=[k for k in state.get('processedBucketKeys',[]) if k!='2026-06-21|480']
# Keep other keys as-is so the job recalculates only the requested bucket.
json.dump(state, open(p,'w',encoding='utf-8'), ensure_ascii=False, indent=2)
print('removed 2026-06-21|480 from transfer state')
PY
BUS_TRANSFER_TARGET_DATE=2026-06-21 \
BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME=23:59 \
BUS_TRANSFER_MAX_BUCKETS_PER_RUN=1 \
BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_ROUTE_PATTERN=6 \
BUS_TRANSFER_POTENTIAL_SPARK_MASTER=local[2] \
BUS_TRANSFER_POTENTIAL_DRIVER_MEMORY=8g \
BUS_TRANSFER_POTENTIAL_EXECUTOR_MEMORY=8g \
./bin/run-transfer-potential.sh

echo "$(date -Is) accessibility map rebuild started"
BUS_ACCESSIBILITY_SOURCE=transfer-potential \
BUS_TRANSFER_POTENTIAL_DIR=/home/eljah/data/buscrawl/transfer-potential \
BUS_ACCESSIBILITY_ORIGIN_STOP_IDS=12078,12112 \
BUS_ACCESSIBILITY_ORIGIN_STOP=Tasma \
BUS_ACCESSIBILITY_SERVICE_DATE=2026-06-21 \
BUS_ACCESSIBILITY_DEPARTURE_TIME=08:00 \
./bin/run-accessibility-map-cache.sh

echo "$(date -Is) validating transfer targets"
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

echo "$(date -Is) transfer bucket 480/accessibility rebuild finished"
