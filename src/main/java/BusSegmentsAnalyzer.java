import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.*;
import org.json.*;

import java.nio.file.*;
import java.util.*;

public class BusSegmentsAnalyzer {
    static final double STOP_RADIUS = 50.0; // в метрах

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
                .getOrCreate();

        // Телеметрия автобусов
        Dataset<Row> busData = spark.read().parquet("bus-data-parquet/");
        busData.createOrReplaceTempView("bus_data");

        // Загрузка JSON остановок
        String jsonString = Files.readString(Paths.get("src/main/resources/routes.json"));
        JSONObject jsonRoot = new JSONObject(jsonString);
        JSONObject nbusstop = jsonRoot.getJSONObject("nbusstop");

        List<Row> stopRows = new ArrayList<>();
        Iterator<String> keys = nbusstop.keys();
        while (keys.hasNext()) {
            String stopId = keys.next();
            JSONArray arr = nbusstop.getJSONArray(stopId);

            // исправленная последовательность координат: долгота и широта поменяны местами
            stopRows.add(RowFactory.create(
                    stopId,
                    arr.getString(0),
                    arr.getDouble(2),  // широта (latitude), ранее была перепутана
                    arr.getDouble(1),  // долгота (longitude), ранее была перепутана
                    arr.getString(3),
                    arr.getInt(4)
            ));
        }

        StructType stopSchema = new StructType(new StructField[]{
                new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("slug", DataTypes.StringType, false, Metadata.empty()),
                new StructField("routeNumber", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> stopsDF = spark.createDataFrame(stopRows, stopSchema);
        stopsDF.createOrReplaceTempView("stops");

        // Регистрация функции расчёта расстояния
        spark.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>)
                BusSegmentsAnalyzer::haversine, DataTypes.DoubleType);

        // Присоединение телеметрии автобусов к остановкам
        Dataset<Row> dataWithStops = spark.sql(
                "SELECT bd.*, s.stopId, s.name AS stopName, s.routeNumber, " +
                        "haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) AS dist_to_stop " +
                        "FROM bus_data bd CROSS JOIN stops s " +
                        "WHERE haversine(bd.latitude, bd.longitude, s.latitude, s.longitude) <= " + STOP_RADIUS
        );

        // Диагностический вывод датафреймов
        System.out.println("== Список остановок (stopsDF) ==");
        stopsDF.show(false);
        stopsDF.printSchema();

        System.out.println("== Данные телеметрии автобусов (busData) ==");
        busData.show(10, false);
        busData.printSchema();

        System.out.println("== Данные с присоединёнными остановками (dataWithStops) ==");
        dataWithStops.show(10, false);
        dataWithStops.printSchema();

        dataWithStops.createOrReplaceTempView("data_with_stops");

        // Последовательность остановок автобусов
        Dataset<Row> stopEvents = spark.sql(
                "SELECT plate, eventTime, stopId, stopName, routeNumber, " +
                        "ROW_NUMBER() OVER (PARTITION BY plate ORDER BY eventTime) AS seq " +
                        "FROM data_with_stops"
        );
        stopEvents.createOrReplaceTempView("stop_events");

        // Создание сегментов между остановками
        Dataset<Row> segments = spark.sql(
                "SELECT s1.plate, s1.stopId AS start_stop, s2.stopId AS end_stop, " +
                        "s1.stopName AS start_name, s2.stopName AS end_name, " +
                        "s1.eventTime AS start_time, s2.eventTime AS end_time, s1.routeNumber " +
                        "FROM stop_events s1 JOIN stop_events s2 " +
                        "ON s1.plate = s2.plate AND s2.seq = s1.seq + 1"
        );
        segments.createOrReplaceTempView("segments");

        // Добавление детальных точек маршрута и метрик сегментов
        Dataset<Row> detailedSegments = spark.sql(
                "SELECT seg.*, " +
                        "(unix_timestamp(seg.end_time) - unix_timestamp(seg.start_time)) AS duration_sec, " +
                        "(SELECT collect_list(struct(latitude, longitude, speed)) " +
                        " FROM bus_data bd " +
                        " WHERE bd.plate = seg.plate " +
                        " AND bd.eventTime BETWEEN seg.start_time AND seg.end_time) AS points_array " +
                        "FROM segments seg"
        );

        // Вывод сегментов с деталями
        System.out.println("== Итоговые детальные сегменты (detailedSegments) ==");
        detailedSegments.show(false);
        detailedSegments.printSchema();

        spark.stop();
    }
}
