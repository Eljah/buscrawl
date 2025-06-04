import org.apache.spark.sql.*;

public class CountBusPoints {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CountBusPoints")
                .master("local[*]")
                .getOrCreate();

        //Dataset<Row> data = spark.read().parquet("D:/bus-data-parquet/");
        Dataset<Row> data = spark.read().parquet("bus-data-parquet/");
        data.createOrReplaceTempView("buses");

        Dataset<Row> countData = spark.sql(
                "SELECT COUNT(*) AS total_points " +
                        "FROM buses "// +
                        //"WHERE realRouteNumber = '10А' AND eventTime > '2025-05-31 13:00:00'"
        );

        countData.show();

        spark.stop();
    }
}
