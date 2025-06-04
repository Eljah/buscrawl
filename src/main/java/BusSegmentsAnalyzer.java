import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.*;
import org.json.*;

import java.nio.file.*;
import java.sql.Timestamp;
import java.util.*;

public class BusSegmentsAnalyzer {
    static final double STOP_RADIUS = 150.0;

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

        Dataset<Row> busData = spark.read().parquet("bus-data-parquet/");
        busData.createOrReplaceTempView("bus_data");

        String jsonString = Files.readString(Paths.get("src/main/resources/routes.json"));
        JSONObject jsonRoot = new JSONObject(jsonString);

        // Загрузка остановок
        JSONObject nbusstop = jsonRoot.getJSONObject("nbusstop");
        List<Row> stopRows = new ArrayList<>();
        for (String stopId : nbusstop.keySet()) {
            JSONArray arr = nbusstop.getJSONArray(stopId);
            stopRows.add(RowFactory.create(
                    stopId,
                    arr.getString(0),
                    arr.getDouble(2), // latitude
                    arr.getDouble(1), // longitude
                    arr.getString(3)
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

        System.out.println("== stopsDF ==");
        stopsDF.show(5, false);

        // Загрузка маршрутов
        JSONObject routeJson = jsonRoot.getJSONObject("route");
        List<Row> routeRows = new ArrayList<>();
        for (String routeStopKey : routeJson.keySet()) {
            JSONArray routeArr = routeJson.getJSONArray(routeStopKey);
            routeRows.add(RowFactory.create(
                    routeArr.getInt(0),                          // routeId
                    String.valueOf(routeArr.getInt(1)),          // stopId
                    routeArr.getInt(2),                          // direction
                    routeArr.getInt(3)                           // stopOrder
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

        System.out.println("== routesDF ==");
        routesDF.show(5, false);

        spark.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                BusSegmentsAnalyzer::haversine, DataTypes.DoubleType);

        Dataset<Row> dataWithStops = spark.sql(
                "SELECT bd.*, s.stopId, s.name AS stopName, r.routeId, r.stopOrder, r.direction, bd.realRouteNumber, " +
                        "haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) AS dist_to_stop " +
                        "FROM bus_data bd CROSS JOIN stops s " +
                        "JOIN routes r ON r.stopId = s.stopId " +
                        "WHERE haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) <= " + STOP_RADIUS
        );
        dataWithStops.createOrReplaceTempView("data_with_stops");

        System.out.println("== dataWithStops ==");
        dataWithStops.show(5, false);

        Dataset<Row> aggregatedStops = spark.sql(
                "SELECT plate, stopId, stopName, routeId, stopOrder, realRouteNumber, " +
                        "window(eventTime, '30 minutes').start as window_start, " +
                        "min(eventTime) as first_seen, max(eventTime) as last_seen " +
                        "FROM data_with_stops " +
                        "GROUP BY plate, stopId, stopName, routeId, stopOrder, realRouteNumber, window(eventTime, '30 minutes')"
        );
        aggregatedStops.createOrReplaceTempView("aggregated_stops");

        System.out.println("== aggregatedStops ==");
        aggregatedStops.show(5, false);

        Dataset<Row> segments = spark.sql(
                "SELECT s1.plate, s1.stopId AS start_stop, s2.stopId AS end_stop, " +
                        "s1.stopName AS start_name, s2.stopName AS end_name, " +
                        "s1.last_seen AS departure_time, s2.first_seen AS arrival_time, s1.routeId, s1.realRouteNumber " +
                        "FROM aggregated_stops s1 " +
                        "JOIN aggregated_stops s2 ON s1.plate = s2.plate AND s1.routeId = s2.routeId AND s1.realRouteNumber = s2.realRouteNumber " +
                        "WHERE s2.first_seen > s1.last_seen AND s2.stopOrder = s1.stopOrder + 1"
        );
        segments.createOrReplaceTempView("segments");

        System.out.println("== segments ==");
        segments.show(10, false);

        // Получение точек отдельно по небольшим батчам (например, по 5 сегментов)
        List<Row> segmentList = segments.limit(5).collectAsList();
        for (Row segment : segmentList) {
            String plate = segment.getAs("plate");
            Timestamp departure = segment.getAs("departure_time");
            Timestamp arrival = segment.getAs("arrival_time");

            Dataset<Row> points = spark.sql(
                    "SELECT latitude, longitude, speed, eventTime FROM bus_data " +
                            "WHERE plate = '" + plate + "' " +
                            "AND eventTime BETWEEN '" + departure + "' AND '" + arrival + "'"
            );

            System.out.println("== Детали сегмента автобуса " + plate + " от " + departure + " до " + arrival + " ==");
            points.show(false);
        }

        spark.stop();
    }
}
