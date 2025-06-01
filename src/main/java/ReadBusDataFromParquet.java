import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadBusDataFromParquet {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BusDataAnalysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data = spark.read().parquet("bus-data-parquet/");
        data.createOrReplaceTempView("buses");

        spark.sql("SELECT realRouteNumber, plate, eventTime, longitude, latitude, speed " +
                        "FROM buses " +
                        "WHERE realRouteNumber = '10А' AND eventTime > '2025-05-31 13:00:00'")
                .show();
    }
}
