import org.apache.spark.sql.*;

public class SegmentSpeedAnalyzer {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SegmentSpeedAnalyzer")
                .master("local[*]")
                .getOrCreate();

        // Загрузка данных сегментов (предполагается, что уже обработаны)
        Dataset<Row> segments = spark.read().parquet("processed-segments/");

        segments.createOrReplaceTempView("segments");

        // Самое быстрое прохождение каждого участка между остановками
        Dataset<Row> fastestSegments = spark.sql(
                "SELECT start_stop, end_stop, start_name, end_name, plate, routeNumber, duration_sec, start_time, end_time " +
                        "FROM (" +
                        "   SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY duration_sec ASC) AS rn " +
                        "   FROM segments" +
                        ") WHERE rn = 1"
        );

        // Самое медленное прохождение каждого участка между остановками
        Dataset<Row> slowestSegments = spark.sql(
                "SELECT start_stop, end_stop, start_name, end_name, plate, routeNumber, duration_sec, start_time, end_time " +
                        "FROM (" +
                        "   SELECT *, ROW_NUMBER() OVER (PARTITION BY start_stop, end_stop ORDER BY duration_sec DESC) AS rn " +
                        "   FROM segments" +
                        ") WHERE rn = 1"
        );

        // Вывод самых быстрых сегментов
        System.out.println("🚀 Самые быстрые проезды между остановками:");
        fastestSegments.show(false);

        // Вывод самых медленных сегментов
        System.out.println("🐢 Самые медленные проезды между остановками:");
        slowestSegments.show(false);

        spark.stop();
    }
}
