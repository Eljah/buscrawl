import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.*;
import org.json.*;

import java.nio.file.*;
import java.util.*;

public class BusRubberinessAnalyzer {
    static final double STOP_RADIUS = 50.0;

    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371000;
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dphi = Math.toRadians(lat2 - lat1);
        double dlambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dphi / 2) * Math.sin(dphi / 2)
                + Math.cos(phi1) * Math.cos(phi2)
                * Math.sin(dlambda / 2) * Math.sin(dlambda / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("BusRubberinessAnalyzer")
                .master("local[*]")
                .config("spark.driver.memory", "14g")
                .config("spark.executor.memory", "14g")
                .getOrCreate();

        spark.sparkContext().setCheckpointDir("D:/parquet/checkpoints");

        Dataset<Row> busData;
        if (Files.exists(Paths.get("D:/parquet/busData.parquet"))) {
            busData = spark.read().parquet("D:/parquet/busData.parquet");
        } else {
            busData = spark.read().parquet("D:/bus-data-parquet/");
            busData.write().mode("overwrite").parquet("D:/parquet/busData.parquet");
        }
        busData.createOrReplaceTempView("bus_data");

        Dataset<Row> stopsDF;
        if (Files.exists(Paths.get("D:/parquet/stopsDF.parquet"))) {
            stopsDF = spark.read().parquet("D:/parquet/stopsDF.parquet");
        } else {
            String jsonString = Files.readString(Paths.get("src/main/resources/routes.json"));
            JSONObject jsonRoot = new JSONObject(jsonString);

            JSONObject nbusstop = jsonRoot.getJSONObject("nbusstop");
            List<Row> stopRows = new ArrayList<>();
            for (String stopId : nbusstop.keySet()) {
                JSONArray arr = nbusstop.getJSONArray(stopId);
                stopRows.add(RowFactory.create(
                        stopId, arr.getString(0), arr.getDouble(2), arr.getDouble(1), arr.getString(3)
                ));
            }
            StructType stopSchema = new StructType(new StructField[]{
                    new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                    new StructField("slug", DataTypes.StringType, false, Metadata.empty())
            });

            stopsDF = spark.createDataFrame(stopRows, stopSchema);
            stopsDF.write().mode("overwrite").parquet("D:/parquet/stopsDF.parquet");
        }
        stopsDF.createOrReplaceTempView("stops");

        Dataset<Row> routesDF;
        if (Files.exists(Paths.get("D:/parquet/routesDF.parquet"))) {
            routesDF = spark.read().parquet("D:/parquet/routesDF.parquet");
        } else {
            String jsonString = Files.readString(Paths.get("src/main/resources/routes.json"));
            JSONObject jsonRoot = new JSONObject(jsonString);
            JSONObject routeJson = jsonRoot.getJSONObject("route");
            List<Row> routeRows = new ArrayList<>();
            for (String routeStopKey : routeJson.keySet()) {
                JSONArray routeArr = routeJson.getJSONArray(routeStopKey);
                routeRows.add(RowFactory.create(
                        routeArr.getInt(0), String.valueOf(routeArr.getInt(1)), routeArr.getInt(2), routeArr.getInt(3)
                ));
            }
            StructType routeSchema = new StructType(new StructField[]{
                    new StructField("routeId", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("direction", DataTypes.IntegerType, false, Metadata.empty()),
                    new StructField("stopOrder", DataTypes.IntegerType, false, Metadata.empty())
            });
            routesDF = spark.createDataFrame(routeRows, routeSchema);
            routesDF.write().mode("overwrite").parquet("D:/parquet/routesDF.parquet");
        }
        routesDF.createOrReplaceTempView("routes");

        spark.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                BusRubberinessAnalyzer::haversine, DataTypes.DoubleType);

        Dataset<Row> dataWithStops;
        if (Files.exists(Paths.get("D:/parquet/dataWithStops.parquet"))) {
            dataWithStops = spark.read().parquet("D:/parquet/dataWithStops.parquet");
        } else {
            dataWithStops = spark.sql(
                    "SELECT bd.*, s.stopId, s.name AS stopName, s.latitude AS stopLat, s.longitude AS stopLon, " +
                            "r.routeId, r.direction, r.stopOrder, " +
                            "haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) AS dist_to_stop " +
                            "FROM bus_data bd " +
                            "CROSS JOIN stops s " +
                            "JOIN routes r ON r.stopId = s.stopId AND bd.internalRouteId = r.routeId " +
                            "WHERE haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) <= " + STOP_RADIUS
            ).repartition(200).checkpoint();
            dataWithStops.write().mode("overwrite").parquet("D:/parquet/dataWithStops.parquet");
        }
        dataWithStops.createOrReplaceTempView("data_with_stops");

        Dataset<Row> aggregatedStops;
        if (Files.exists(Paths.get("D:/parquet/aggregatedStops.parquet"))) {
            aggregatedStops = spark.read().parquet("D:/parquet/aggregatedStops.parquet");
        } else {
            aggregatedStops = spark.sql(
                    "SELECT plate, stopId, stopName, routeId, direction, stopOrder, " +
                            "window(eventTime, '15 minutes').start as window_start, " +
                            "MIN(eventTime) as first_seen, MAX(eventTime) as last_seen, " +
                            "FIRST(stopLat) AS stop_lat, FIRST(stopLon) AS stop_lon " +
                            "FROM data_with_stops " +
                            "GROUP BY plate, stopId, stopName, routeId, direction, stopOrder, window(eventTime, '15 minutes')"
            ).checkpoint();
            aggregatedStops.write().mode("overwrite").parquet("D:/parquet/aggregatedStops.parquet");
        }
        aggregatedStops.createOrReplaceTempView("aggregated_stops");

        Dataset<Row> segments;
        if (Files.exists(Paths.get("D:/parquet/segments_adj.parquet"))) {
            segments = spark.read().parquet("D:/parquet/segments_adj.parquet");
        } else {
            segments = spark.sql(
                    "SELECT s1.plate, s1.stopId AS start_stop, s2.stopId AS end_stop, s1.stopName AS start_name, s2.stopName AS end_name, " +
                            "s1.last_seen AS departure_time, s2.first_seen AS arrival_time, " +
                            "(unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen)) AS duration_sec, " +
                            "s1.routeId, s1.direction, " +
                            "s1.stop_lat AS start_stop_lat, s1.stop_lon AS start_stop_lon, s2.stop_lat AS end_stop_lat, s2.stop_lon AS end_stop_lon, " +
                            "(haversine(s1.stop_lat, s1.stop_lon, s2.stop_lat, s2.stop_lon) / " +
                            "(unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen))) * 3.6 AS avg_segment_speed_kmh " +
                            "FROM aggregated_stops s1 " +
                            "JOIN aggregated_stops s2 ON s1.plate = s2.plate AND s1.routeId = s2.routeId " +
                            "AND s1.direction = s2.direction AND s2.stopOrder = s1.stopOrder + 1 " +
                            "WHERE s2.first_seen > s1.last_seen AND (unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen)) <= 1800"
            ).checkpoint();
            segments.write().mode("overwrite").parquet("D:/parquet/segments_adj.parquet");
        }
        segments.createOrReplaceTempView("segments");

        Dataset<Row> rubberStats;
        if (Files.exists(Paths.get("D:/parquet/rubberStats.parquet"))) {
            rubberStats = spark.read().parquet("D:/parquet/rubberStats.parquet");
        } else {
            rubberStats = spark.sql(
                    "WITH ordered AS (" +
                            "SELECT *, " +
                            "LEAD(departure_time) OVER (PARTITION BY plate, routeId, direction ORDER BY departure_time) AS next_dep, " +
                            "LEAD(start_stop) OVER (PARTITION BY plate, routeId, direction ORDER BY departure_time) AS next_start " +
                            "FROM segments" +
                        ") " +
                        "SELECT routeId, direction, " +
                            "SUM(duration_sec) AS moving_sec, " +
                            "SUM(CASE WHEN next_start = end_stop THEN unix_timestamp(next_dep) - unix_timestamp(arrival_time) ELSE 0 END) AS waiting_sec, " +
                            "(SUM(CASE WHEN next_start = end_stop THEN unix_timestamp(next_dep) - unix_timestamp(arrival_time) ELSE 0 END) / SUM(duration_sec)) AS rubberiness " +
                        "FROM ordered GROUP BY routeId, direction"
            ).checkpoint();
            rubberStats.write().mode("overwrite").parquet("D:/parquet/rubberStats.parquet");
        }
        rubberStats.show(false);

        spark.stop();
    }
}
