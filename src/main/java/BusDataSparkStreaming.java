import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.streaming.Trigger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;

public class BusDataSparkStreaming {
    public static void main(String[] args) throws StreamingQueryException {
        String ingestSource = System.getenv().getOrDefault("BUS_INGEST_SOURCE", "spool");
        String tcpHost = System.getenv().getOrDefault("BUS_TCP_HOST", "localhost");
        int tcpPort = Integer.parseInt(System.getenv().getOrDefault("BUS_TCP_PORT", "9999"));
        Path storageRoot = Paths.get(System.getenv().getOrDefault("BUS_STORAGE_ROOT", "./var/bus"));
        Path localDir = storageRoot.resolve("spark-temp");
        Path outputDir = storageRoot.resolve("bus-data-parquet");
        Path checkpointDir = storageRoot.resolve("bus-data-checkpoint");
        Path spoolReadyDir = Paths.get(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_READY_DIR",
                storageRoot.resolve("raw-json-spool").resolve("ready").toString()
        ));
        int maxFilesPerTrigger = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_MAX_FILES_PER_TRIGGER",
                "64"
        ));
        int outputFilesPerBatch = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_PARQUET_OUTPUT_FILES_PER_BATCH",
                "1"
        ));

        try {
            Files.createDirectories(localDir);
            Files.createDirectories(outputDir);
            Files.createDirectories(checkpointDir);
            Files.createDirectories(spoolReadyDir);
        } catch (Exception e) {
            throw new RuntimeException("Failed to prepare storage directories", e);
        }

        SparkSession spark = SparkSession.builder()
                .appName("BusDataStreaming")
                .master("local[*]")
                .config("spark.local.dir", localDir.toAbsolutePath().toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.driver.memory", "10g")
                .config("spark.executor.memory", "10g")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("internalRouteId", DataTypes.StringType)
                .add("realRouteNumber", DataTypes.StringType)
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("speed", DataTypes.IntegerType)
                .add("plate", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("readableTime", DataTypes.StringType)
                .add("sourceTimestamp", DataTypes.LongType)
                .add("sourceReadableTime", DataTypes.StringType);

        Dataset<String> lines;
        if ("socket".equalsIgnoreCase(ingestSource)) {
            System.out.printf("Spark started, reading raw events from TCP %s:%d%n", tcpHost, tcpPort);
            lines = spark.readStream()
                    .format("socket")
                    .option("host", tcpHost)
                    .option("port", tcpPort)
                    .load()
                    .as(Encoders.STRING());
        } else if ("spool".equalsIgnoreCase(ingestSource)) {
            System.out.println("Spark started, reading raw events from disk spool: "
                    + spoolReadyDir.toAbsolutePath());
            lines = spark.readStream()
                    .format("text")
                    .option("maxFilesPerTrigger", maxFilesPerTrigger)
                    .load(spoolReadyDir.toAbsolutePath().toString())
                    .as(Encoders.STRING());
        } else {
            throw new IllegalArgumentException("Unsupported BUS_INGEST_SOURCE: " + ingestSource);
        }

        Dataset<Row> busData = lines.select(from_json(col("value"), schema).alias("data"))
                .filter(col("data").isNotNull())
                .select("data.*")
                .withColumn("eventTime", expr("timestamp_seconds(timestamp)"));

        try {
            busData.writeStream()
                    .foreachBatch((dataset, batchId) -> {
                        System.out.printf("Batch #%d received%n", batchId);
                        dataset.dropDuplicates(
                                        "internalRouteId",
                                        "realRouteNumber",
                                        "plate",
                                        "eventTime",
                                        "latitude",
                                        "longitude"
                                )
                                .coalesce(Math.max(1, outputFilesPerBatch))
                                .write()
                                .mode("append")
                                .parquet(outputDir.toAbsolutePath().toString());
                    })
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .option("checkpointLocation", checkpointDir.toAbsolutePath().toString())
                    .start()
                    .awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
