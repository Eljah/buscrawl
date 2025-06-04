import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.*;
import org.json.*;

import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;

public class BusSegmentsAnalyzer {
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
                .appName("BusSegmentsAnalyzer")
                .master("local[*]")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .getOrCreate();

        // Bus telemetry
        Dataset<Row> busData = spark.read().parquet("bus-data-parquet/");
        busData.createOrReplaceTempView("bus_data");
        System.out.println("=== busData ===");
        busData.show(5, false);

        // JSON data loading
        String jsonString = Files.readString(Paths.get("src/main/resources/routes.json"));
        JSONObject jsonRoot = new JSONObject(jsonString);

        // Loading stops
        JSONObject nbusstop = jsonRoot.getJSONObject("nbusstop");
        List<Row> stopRows = new ArrayList<>();
        for (String stopId : nbusstop.keySet()) {
            JSONArray arr = nbusstop.getJSONArray(stopId);
            stopRows.add(RowFactory.create(
                    stopId, arr.getString(0),
                    arr.getDouble(2), arr.getDouble(1), arr.getString(3)
            ));
        }

        StructType stopSchema = new StructType(new StructField[]{
                new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("slug", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> stopsDF = spark.createDataFrame(stopRows, stopSchema);
        stopsDF.createOrReplaceTempView("stops");
        System.out.println("=== stopsDF ===");
        stopsDF.show(5, false);

        // Loading routes
        JSONObject routeJson = jsonRoot.getJSONObject("route");
        List<Row> routeRows = new ArrayList<>();
        for (String routeStopKey : routeJson.keySet()) {
            JSONArray routeArr = routeJson.getJSONArray(routeStopKey);
            routeRows.add(RowFactory.create(
                    routeArr.getInt(0), String.valueOf(routeArr.getInt(1)),
                    routeArr.getInt(2), routeArr.getInt(3)
            ));
        }

        StructType routeSchema = new StructType(new StructField[]{
                new StructField("routeId", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("direction", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("stopOrder", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> routesDF = spark.createDataFrame(routeRows, routeSchema);
        routesDF.createOrReplaceTempView("routes");
        System.out.println("=== routesDF ===");
        routesDF.show(5, false);

        // Register UDF
        spark.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                BusSegmentsAnalyzer::haversine, DataTypes.DoubleType);

        // Telemetry joined with stops and routes
        Dataset<Row> dataWithStops = spark.sql(
                "SELECT bd.*, s.stopId, s.name AS stopName, r.routeId, r.stopOrder, r.direction, bd.realRouteNumber, " +
                        "haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) AS dist_to_stop " +
                        "FROM bus_data bd CROSS JOIN stops s " +
                        "JOIN routes r ON r.stopId = s.stopId AND bd.internalRouteId = r.routeId " +
                        "WHERE haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) <= " + STOP_RADIUS
        );
        dataWithStops.createOrReplaceTempView("data_with_stops");
        System.out.println("=== dataWithStops ===");
        dataWithStops.show(5, false);

        // Aggregated stops
        Dataset<Row> aggregatedStops = spark.sql(
                "SELECT plate, stopId, stopName, routeId, stopOrder, realRouteNumber, " +
                        "window(eventTime, '30 minutes').start as window_start, " +
                        "MIN(eventTime) as first_seen, MAX(eventTime) as last_seen, " +
                        "FIRST(latitude) AS first_lat, FIRST(longitude) AS first_lon, " +
                        "LAST(latitude) AS last_lat, LAST(longitude) AS last_lon " +
                        "FROM data_with_stops " +
                        "GROUP BY plate, stopId, stopName, routeId, stopOrder, realRouteNumber, window(eventTime, '30 minutes')"
        );
        aggregatedStops.createOrReplaceTempView("aggregated_stops");

        System.out.println("=== aggregatedStops ===");
        aggregatedStops.show(5, false);

// Создание сегментов с координатами и средней скоростью между остановками
        Dataset<Row> segments = spark.sql(
                "SELECT s1.plate, " +
                        "s1.stopId AS start_stop, s2.stopId AS end_stop, " +
                        "s1.stopName AS start_name, s2.stopName AS end_name, " +
                        "s1.last_seen AS departure_time, s2.first_seen AS arrival_time, " +
                        "CAST((unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen)) AS INT) AS duration_sec, " +
                        "s1.routeId, s1.realRouteNumber, " +
                        "s1.last_lat AS start_lat, s1.last_lon AS start_lon, " +
                        "s2.first_lat AS end_lat, s2.first_lon AS end_lon, " +
                        "(haversine(s1.last_lat, s1.last_lon, s2.first_lat, s2.first_lon) / " +
                        "(unix_timestamp(s2.first_seen) - unix_timestamp(s1.last_seen))) * 3.6 AS avg_segment_speed_kmh " +
                        "FROM aggregated_stops s1 " +
                        "JOIN aggregated_stops s2 ON s1.plate = s2.plate AND s1.routeId = s2.routeId AND s1.realRouteNumber = s2.realRouteNumber " +
                        "WHERE s2.first_seen > s1.last_seen AND s2.stopOrder = s1.stopOrder + 1"
        );
        segments.createOrReplaceTempView("segments");

        System.out.println("=== segments ===");
        segments.show(5, false);

// Последняя часть (детальные точки маршрута)
        for (Row segment : segments.limit(5).collectAsList()) {
            String plate = segment.getString(0);
            Timestamp departure = segment.getTimestamp(5);
            Timestamp arrival = segment.getTimestamp(6);
            String startName = segment.getString(3);
            String endName = segment.getString(4);

            Dataset<Row> points = spark.sql(
                    "SELECT DISTINCT latitude, longitude, speed, eventTime FROM bus_data " +
                            "WHERE plate = '" + plate + "' AND eventTime BETWEEN '" + departure + "' AND '" + arrival + "'"
            );
            points.createOrReplaceTempView("segment_points");

            Row coords = spark.sql(
                    "SELECT " +
                            "(SELECT latitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lat, " +
                            "(SELECT longitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lon, " +
                            "(SELECT latitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lat, " +
                            "(SELECT longitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lon, " +
                            "AVG(speed) AS avg_speed FROM segment_points"
            ).first();

            System.out.printf("\n=== Сегмент %s (%s → %s) ===\n", plate, startName, endName);
            System.out.printf("Start coords: %.6f, %.6f\n", coords.getDouble(0), coords.getDouble(1));
            System.out.printf("End coords: %.6f, %.6f\n", coords.getDouble(2), coords.getDouble(3));
            System.out.printf("Avg speed: %.2f km/h\n", coords.getDouble(4));
            points.orderBy("eventTime").show(false);
        }
        for (Row segment : segments.limit(5).collectAsList()) {
            String plate = segment.getString(0);
            Timestamp departure = segment.getTimestamp(5);
            Timestamp arrival = segment.getTimestamp(6);
            String startName = segment.getString(3);
            String endName = segment.getString(4);

            Dataset<Row> points = spark.sql(
                    "SELECT DISTINCT latitude, longitude, speed, eventTime FROM bus_data " +
                            "WHERE plate = '" + plate + "' AND eventTime BETWEEN '" + departure + "' AND '" + arrival + "'"
            );
            points.createOrReplaceTempView("segment_points");

            Row coords = spark.sql(
                    "SELECT " +
                            "(SELECT latitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lat, " +
                            "(SELECT longitude FROM segment_points ORDER BY eventTime ASC LIMIT 1) AS start_lon, " +
                            "(SELECT latitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lat, " +
                            "(SELECT longitude FROM segment_points ORDER BY eventTime DESC LIMIT 1) AS end_lon, " +
                            "AVG(speed) AS avg_speed FROM segment_points"
            ).first();

            System.out.printf("\n=== Сегмент %s (%s → %s) ===\n", plate, startName, endName);
            System.out.printf("Start coords: %.6f, %.6f\n", coords.getDouble(0), coords.getDouble(1));
            System.out.printf("End coords: %.6f, %.6f\n", coords.getDouble(2), coords.getDouble(3));
            System.out.printf("Avg speed: %.2f km/h\n", coords.getDouble(4));
            points.orderBy("eventTime").show(false);
        }

        Dataset<Row> speedStats = spark.sql(
                "SELECT " +
                        "fast.start_stop, fast.end_stop, fast.start_name, fast.end_name, " +
                        "fast.plate AS fast_plate, fast.routeId AS fast_routeId, fast.realRouteNumber AS fast_realRouteNumber, " +
                        "fast.avg_segment_speed_kmh AS max_speed, " +
                        "slow.plate AS slow_plate, slow.routeId AS slow_routeId, slow.realRouteNumber AS slow_realRouteNumber, " +
                        "slow.avg_segment_speed_kmh AS min_speed, " +
                        "(fast.avg_segment_speed_kmh / slow.avg_segment_speed_kmh) AS speed_ratio " +
                        "FROM ( " +
                        "SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY avg_segment_speed_kmh DESC) AS rn_fast " +
                        "FROM segments" +
                        ") fast " +
                        "JOIN ( " +
                        "SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY avg_segment_speed_kmh ASC) AS rn_slow " +
                        "FROM segments" +
                        ") slow " +
                        "ON fast.start_stop = slow.start_stop AND fast.end_stop = slow.end_stop " +
                        "WHERE fast.rn_fast = 1 AND slow.rn_slow = 1 " +
                        "AND fast.plate <> slow.plate"
        );

        speedStats.createOrReplaceTempView("speed_stats");


        speedStats.createOrReplaceTempView("speed_stats");

        Dataset<Row> extremeSegments = spark.sql(
                "SELECT DISTINCT s.* FROM segments s " +
                        "JOIN speed_stats stats ON s.start_stop = stats.start_stop AND s.end_stop = stats.end_stop " +
                        "WHERE s.avg_segment_speed_kmh = stats.max_speed OR s.avg_segment_speed_kmh = stats.min_speed"
        );

        extremeSegments.createOrReplaceTempView("extreme_segments");

        System.out.println("=== ТОП-20 сегментов с наибольшим различием скоростей ===");
        spark.sql(
                "SELECT * FROM speed_stats ORDER BY speed_ratio DESC LIMIT 20"
        ).show(false);

        System.out.println("=== ТОП-20 сегментов с наименьшим различием скоростей ===");
        spark.sql(
                "SELECT * FROM speed_stats ORDER BY speed_ratio ASC LIMIT 20"
        ).show(false);


        spark.stop();
    }
}
