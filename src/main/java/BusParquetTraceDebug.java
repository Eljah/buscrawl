import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class BusParquetTraceDebug {
    public static void main(String[] args) {
        Path parquetDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        SparkSession spark = SparkSession.builder()
                .appName("BusParquetTraceDebug")
                .master(System.getenv().getOrDefault("BUS_DEBUG_SPARK_MASTER", "local[2]"))
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> data = spark.read().parquet(parquetDir.toAbsolutePath().toString())
                    .select("plate", "internalRouteId", "realRouteNumber", "eventTime", "timestamp");

            data.agg(
                    max("eventTime").alias("max_event_time"),
                    max("timestamp").alias("max_timestamp")
            ).show(false);

            data.filter(col("eventTime").isNotNull())
                    .orderBy(col("eventTime").desc())
                    .show(10, false);
        } finally {
            spark.stop();
        }
    }
}
