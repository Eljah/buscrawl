import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.to_timestamp;

public class BusDataSparkStreaming {
    public static void main(String[] args) throws StreamingQueryException {
        String tcpHost = System.getenv().getOrDefault("BUS_TCP_HOST", "localhost");
        int tcpPort = Integer.parseInt(System.getenv().getOrDefault("BUS_TCP_PORT", "9999"));
        Path storageRoot = Paths.get(System.getenv().getOrDefault("BUS_STORAGE_ROOT", "./var/bus"));
        Path localDir = storageRoot.resolve("spark-temp");
        Path outputDir = storageRoot.resolve("bus-data-parquet");
        Path checkpointDir = storageRoot.resolve("bus-data-checkpoint");

        try {
            Files.createDirectories(localDir);
            Files.createDirectories(outputDir);
            Files.createDirectories(checkpointDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare storage directories", e);
        }

        SparkSession spark = SparkSession.builder()
                .appName("BusDataStreaming")
                .master("local[*]")
                .config("spark.local.dir", localDir.toAbsolutePath().toString())
                .config("spark.driver.memory", "10g")
                .config("spark.executor.memory", "10g")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Spark started, connecting to TCP server...");

        StructType schema = new StructType()
                .add("internalRouteId", DataTypes.StringType)
                .add("realRouteNumber", DataTypes.StringType)
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("speed", DataTypes.IntegerType)
                .add("plate", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("readableTime", DataTypes.StringType);

        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", tcpHost)
                .option("port", tcpPort)
                .load()
                .as(Encoders.STRING());

        Dataset<Row> busData = lines.select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withColumn("eventTime", to_timestamp(col("readableTime"), "yyyy-MM-dd HH:mm:ss")
                        .cast(DataTypes.TimestampType));

        try {
            busData.writeStream()
                    .foreachBatch((dataset, batchId) -> {
                        Dataset<Row> persisted = dataset.persist(StorageLevel.MEMORY_AND_DISK());
                        long rows = persisted.count();
                        System.out.printf("Batch #%d received. Rows: %d%n", batchId, rows);
                        persisted.write()
                                .mode("append")
                                .parquet(outputDir.toAbsolutePath().toString());
                        persisted.unpersist();
                    })
                    .option("checkpointLocation", checkpointDir.toAbsolutePath().toString())
                    .start()
                    .awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
