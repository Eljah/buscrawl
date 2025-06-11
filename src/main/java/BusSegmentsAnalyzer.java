import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.*;
import org.json.*;

import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;

public class BusSegmentsAnalyzer {
    static final double STOP_RADIUS = 50.0;

    protected static double haversine(double lat1, double lon1, double lat2, double lon2) {
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
                .appName("BusSegmentsAnalyzer")
                .master("local[*]")
                .config("spark.driver.memory", "14g")
                .config("spark.executor.memory", "14g")
                .getOrCreate();

        spark.sparkContext().setCheckpointDir("D:/parquet/checkpoints");

        // busData
        Dataset<Row> busData;
        if (Files.exists(Paths.get("D:/parquet/busData.parquet"))) {
            busData = spark.read().parquet("D:/parquet/busData.parquet");
        } else {
            busData = spark.read().parquet("D:/bus-data-parquet/");
            busData.write().mode("overwrite").parquet("D:/parquet/busData.parquet");
        }
        busData.createOrReplaceTempView("bus_data");
        busData.show(5, false);

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
        System.out.println("=== stopsDF ===");
        stopsDF.show(5, false);

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
        System.out.println("=== routesDF ===");
        routesDF.show(5, false);

        spark.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                BusSegmentsAnalyzer::haversine, DataTypes.DoubleType);

        Dataset<Row> dataWithStops;
        if (Files.exists(Paths.get("D:/parquet/dataWithStops.parquet"))) {
            dataWithStops = spark.read().parquet("D:/parquet/dataWithStops.parquet");
        } else {
            dataWithStops = spark.sql(
                    "SELECT bd.*, s.stopId, s.name AS stopName, s.latitude AS stopLat, s.longitude AS stopLon, " +
                            "r.routeId, r.stopOrder, haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) AS dist_to_stop " +
                            "FROM bus_data bd " +
                            "CROSS JOIN stops s " +
                            "JOIN routes r ON r.stopId = s.stopId AND bd.internalRouteId = r.routeId " +
                            "WHERE haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) <= " + STOP_RADIUS
            ).repartition(200).checkpoint();
            dataWithStops.write().mode("overwrite").parquet("D:/parquet/dataWithStops.parquet");
        }
        dataWithStops.createOrReplaceTempView("data_with_stops");
        System.out.println("=== dataWithStops ===");
        dataWithStops.show(5, false);

        Dataset<Row> aggregatedStops;
        if (Files.exists(Paths.get("D:/parquet/aggregatedStops.parquet"))) {
            aggregatedStops = spark.read().parquet("D:/parquet/aggregatedStops.parquet");
        } else {
            aggregatedStops = spark.sql(
                    "SELECT plate, stopId, stopName, routeId, stopOrder, " +
                            "window(eventTime, '15 minutes').start as window_start, " +
                            "MIN(eventTime) as first_seen, MAX(eventTime) as last_seen, " +
                            "FIRST(stopLat) AS stop_lat, FIRST(stopLon) AS stop_lon " +
                            "FROM data_with_stops " +
                            "GROUP BY plate, stopId, stopName, routeId, stopOrder, window(eventTime, '15 minutes')"
            ).checkpoint();
            aggregatedStops.write().mode("overwrite").parquet("D:/parquet/aggregatedStops.parquet");
        }
        aggregatedStops.createOrReplaceTempView("aggregated_stops");
        System.out.println("=== aggregatedStops ===");
        aggregatedStops.show(5, false);

        // segments
        Dataset<Row> segments;
        if (Files.exists(Paths.get("D:/parquet/segments.parquet"))) {
            segments = spark.read().parquet("D:/parquet/segments.parquet");
        } else {
            segments = spark.sql(
                    "SELECT s1.plate, s1.stopId AS start_stop, s2.stopId AS end_stop, s1.stopName AS start_name, s2.stopName AS end_name, " +
                            "s1.last_seen AS departure_time, s2.first_seen AS arrival_time, " +
                            "(unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen)) AS duration_sec, s1.routeId, " +
                            "s1.stop_lat AS start_stop_lat, s1.stop_lon AS start_stop_lon, s2.stop_lat AS end_stop_lat, s2.stop_lon AS end_stop_lon, " +
                            "(haversine(s1.stop_lat, s1.stop_lon, s2.stop_lat, s2.stop_lon) / " +
                            "(unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen))) * 3.6 AS avg_segment_speed_kmh " +
                            "FROM aggregated_stops s1 " +
                            "JOIN aggregated_stops s2 ON s1.plate = s2.plate AND s1.routeId = s2.routeId " +
                            "WHERE s2.first_seen > s1.last_seen AND s2.stopOrder = s1.stopOrder + 1"
            ).checkpoint();
            segments.write().mode("overwrite").parquet("D:/parquet/segments.parquet");
        }
        segments.createOrReplaceTempView("segments");
        segments.show(5, false);

        // speedStats
        Dataset<Row> speedStats;
        if (Files.exists(Paths.get("D:/parquet/speedStats.parquet"))) {
            speedStats = spark.read().parquet("D:/parquet/speedStats.parquet");
        } else {
            speedStats = spark.sql(
                    "SELECT fast.*, slow.plate as slow_plate, slow.departure_time as slow_departure_time, " +
                            "slow.arrival_time as slow_arrival_time, slow.duration_sec as slow_duration_sec, " +
                            "slow.avg_segment_speed_kmh as slow_avg_segment_speed_kmh, " +
                            "(fast.avg_segment_speed_kmh / slow.avg_segment_speed_kmh) AS speed_ratio " +
                            "FROM (" +
                            "SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY avg_segment_speed_kmh DESC) AS rn FROM segments" +
                            ") fast " +
                            "JOIN (" +
                            "SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY avg_segment_speed_kmh ASC) AS rn FROM segments" +
                            ") slow " +
                            "ON fast.start_stop = slow.start_stop AND fast.end_stop = slow.end_stop " +
                            "WHERE fast.rn = 1 AND slow.rn = 1 AND fast.plate <> slow.plate"
            ).checkpoint();
            speedStats.write().mode("overwrite").parquet("D:/parquet/speedStats.parquet");
        }
        speedStats.createOrReplaceTempView("speed_stats");
        speedStats.show(5, false);

        // ТОП-20 сегментов с наибольшим различием скоростей
        System.out.println("=== ТОП-20 сегментов с наибольшим различием скоростей ===");
        spark.sql("SELECT * FROM speed_stats ORDER BY speed_ratio DESC LIMIT 20").show(false);

        // ТОП-20 сегментов с наименьшим различием скоростей
        System.out.println("=== ТОП-20 сегментов с наименьшим различием скоростей ===");
        spark.sql("SELECT * FROM speed_stats ORDER BY speed_ratio ASC LIMIT 20").show(false);

        spark.stop();
    }

    // Здесь сохраняются твои оригинальные закомментированные блоки
    // (продолжение в следующем сообщении из-за ограничения длины)
}


// Последняя часть (детальные точки маршрута)
//        for (Row segment : segments.limit(5).collectAsList()) {
//            String plate = segment.getString(0);
//            Timestamp departure = segment.getTimestamp(5);
//            Timestamp arrival = segment.getTimestamp(6);
//            String startName = segment.getString(3);
//            String endName = segment.getString(4);
//
//            Dataset<Row> points = spark.sql(
//                    "SELECT DISTINCT latitude, longitude, speed, eventTime FROM bus_data " +
//                            "WHERE plate = '" + plate + "' AND eventTime BETWEEN '" + departure + "' AND '" + arrival + "'"
//            );
//            points.createOrReplaceTempView("segment_points");
//
//            Row coords = spark.sql(
//                    "SELECT " +
//                            "(SELECT latitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lat, " +
//                            "(SELECT longitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lon, " +
//                            "(SELECT latitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lat, " +
//                            "(SELECT longitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lon, " +
//                            "AVG(speed) AS avg_speed FROM segment_points"
//            ).first();
//
//            System.out.printf("\n=== Сегмент %s (%s → %s) ===\n", plate, startName, endName);
//            System.out.printf("Start coords: %.6f, %.6f\n", coords.getDouble(0), coords.getDouble(1));
//            System.out.printf("End coords: %.6f, %.6f\n", coords.getDouble(2), coords.getDouble(3));
//            System.out.printf("Avg speed: %.2f km/h\n", coords.getDouble(4));
//            points.orderBy("eventTime").show(false);
//        }


//        for (Row segment : segments.limit(5).collectAsList()) {
//            String plate = segment.getString(0);
//            Timestamp departure = segment.getTimestamp(5);
//            Timestamp arrival = segment.getTimestamp(6);
//            String startName = segment.getString(3);
//            String endName = segment.getString(4);
//
//            Dataset<Row> points = spark.sql(
//                    "SELECT DISTINCT latitude, longitude, speed, eventTime FROM bus_data " +
//                            "WHERE plate = '" + plate + "' AND eventTime BETWEEN '" + departure + "' AND '" + arrival + "'"
//            );
//            points.createOrReplaceTempView("segment_points");
//
//            Row coords = spark.sql(
//                    "SELECT " +
//                            "(SELECT latitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lat, " +
//                            "(SELECT longitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lon, " +
//                            "(SELECT latitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lat, " +
//                            "(SELECT longitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lon, " +
//                            "AVG(speed) AS avg_speed FROM segment_points"
//            ).first();
//
//            System.out.printf("\n=== Сегмент %s (%s → %s) ===\n", plate, startName, endName);
//            System.out.printf("Start coords: %.6f, %.6f\n", coords.getDouble(0), coords.getDouble(1));
//            System.out.printf("End coords: %.6f, %.6f\n", coords.getDouble(2), coords.getDouble(3));
//            System.out.printf("Avg speed: %.2f km/h\n", coords.getDouble(4));
//            points.orderBy("eventTime").show(false);
//        }
//
