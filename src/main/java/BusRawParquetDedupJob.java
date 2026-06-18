import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.to_date;

public class BusRawParquetDedupJob {
    public static void main(String[] args) throws Exception {
        Path inputDir = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_INPUT_DIR",
                "./var/bus/bus-data-parquet"
        ));
        Path outputDir = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_OUTPUT_DIR",
                "./var/bus/bus-data-parquet-dedup"
        ));
        Path tempRoot = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_TEMP_ROOT",
                outputDir.getParent().resolve("bus-data-parquet-dedup-tmp").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_SPARK_LOCAL_DIR",
                "./var/bus/raw-dedup-spark-temp"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_RAW_DEDUP_SPARK_MASTER", "local[2]");
        String cityTimezone = System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow");
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_OUTPUT_PARTITIONS",
                "64"
        ));
        String startDate = System.getenv("BUS_RAW_DEDUP_START_DATE");
        String endDate = System.getenv("BUS_RAW_DEDUP_END_DATE");
        boolean byServiceDate = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_DEDUP_BY_SERVICE_DATE",
                "true"
        ));

        if (!Files.exists(inputDir)) {
            throw new IllegalStateException("Raw parquet input directory not found: " + inputDir);
        }
        Files.createDirectories(outputDir);
        Files.createDirectories(tempRoot);
        Files.createDirectories(sparkLocalDir);

        SparkSession spark = SparkSession.builder()
                .appName("BusRawParquetDedupJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_RAW_DEDUP_DRIVER_MEMORY", "8g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_RAW_DEDUP_EXECUTOR_MEMORY", "8g"))
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Path tempOutput = tempRoot.resolve("dedup-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID());

        try {
            Dataset<Row> raw = spark.read()
                    .option("recursiveFileLookup", "true")
                    .parquet(inputDir.toAbsolutePath().toString());

            if (!hasColumn(raw, "eventTime")) {
                raw = raw.withColumn("eventTime", org.apache.spark.sql.functions.expr("timestamp_seconds(timestamp)"));
            }
            if (!hasColumn(raw, "sourceTimestamp")) {
                raw = raw.withColumn("sourceTimestamp", lit(null).cast(DataTypes.LongType));
            }
            if (!hasColumn(raw, "sourceReadableTime")) {
                raw = raw.withColumn("sourceReadableTime", lit(null).cast(DataTypes.StringType));
            }
            if (!hasColumn(raw, "readableTime")) {
                raw = raw.withColumn("readableTime", lit(null).cast(DataTypes.StringType));
            }

            Dataset<Row> withServiceDate = raw
                    .filter(col("timestamp").isNotNull())
                    .filter(col("plate").isNotNull())
                    .filter(col("internalRouteId").isNotNull())
                    .filter(col("latitude").isNotNull())
                    .filter(col("longitude").isNotNull())
                    .withColumn("serviceDate", to_date(from_utc_timestamp(col("eventTime"), cityTimezone)));

            if (startDate != null && !startDate.isBlank()) {
                withServiceDate = withServiceDate.filter(col("serviceDate").geq(lit(startDate)));
            }
            if (endDate != null && !endDate.isBlank()) {
                withServiceDate = withServiceDate.filter(col("serviceDate").leq(lit(endDate)));
            }

            Column[] identity = new Column[]{
                    col("internalRouteId"),
                    col("realRouteNumber"),
                    col("plate"),
                    col("latitude"),
                    col("longitude"),
                    col("speed"),
                    col("sourceTimestamp")
            };

            if (byServiceDate) {
                List<String> serviceDates = withServiceDate
                        .selectExpr("CAST(serviceDate AS STRING) AS serviceDate")
                        .distinct()
                        .sort("serviceDate")
                        .as(Encoders.STRING())
                        .collectAsList();
                System.out.printf("BusRawParquetDedupJob: processing %d service dates%n", serviceDates.size());
                for (String serviceDate : serviceDates) {
                    System.out.printf("BusRawParquetDedupJob: deduplicating serviceDate=%s%n", serviceDate);
                    Dataset<Row> dedup = deduplicate(withServiceDate.filter(col("serviceDate").equalTo(lit(serviceDate))), identity, cityTimezone)
                            .repartition(Math.max(1, outputPartitions / Math.max(1, serviceDates.size())), col("serviceDate"));
                    dedup.write()
                            .mode("append")
                            .partitionBy("serviceDate")
                            .parquet(tempOutput.toAbsolutePath().toString());
                    System.out.printf("BusRawParquetDedupJob: wrote serviceDate=%s%n", serviceDate);
                }
            } else {
                Dataset<Row> dedup = deduplicate(withServiceDate, identity, cityTimezone)
                        .repartition(outputPartitions, col("serviceDate"));
                dedup.write()
                        .mode("overwrite")
                        .partitionBy("serviceDate")
                        .parquet(tempOutput.toAbsolutePath().toString());
            }

            System.out.printf(
                    "BusRawParquetDedupJob: wrote deduplicated raw parquet to %s%n",
                    tempOutput.toAbsolutePath()
            );
            System.out.printf(
                    "BusRawParquetDedupJob: move %s to %s after validation, or set BUS_RAW_DEDUP_OUTPUT_DIR to the final target in a controlled cutover%n",
                    tempOutput.toAbsolutePath(),
                    outputDir.toAbsolutePath()
            );
        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> deduplicate(Dataset<Row> dataset, Column[] identity, String cityTimezone) {
        return dataset
                .groupBy(identity)
                .agg(
                        min("timestamp").alias("timestamp"),
                        min("readableTime").alias("readableTime"),
                        min("sourceReadableTime").alias("sourceReadableTime"),
                        min("eventTime").alias("eventTime")
                )
                .withColumn("serviceDate", to_date(from_utc_timestamp(col("eventTime"), cityTimezone)))
                .select(
                        col("plate"),
                        col("internalRouteId"),
                        col("realRouteNumber"),
                        col("latitude"),
                        col("longitude"),
                        col("eventTime"),
                        col("speed"),
                        col("timestamp"),
                        col("readableTime"),
                        col("sourceTimestamp"),
                        col("sourceReadableTime"),
                        col("serviceDate")
                );
    }

    private static boolean hasColumn(Dataset<Row> dataset, String columnName) {
        for (String column : dataset.columns()) {
            if (column.equals(columnName)) {
                return true;
            }
        }
        return false;
    }
}
