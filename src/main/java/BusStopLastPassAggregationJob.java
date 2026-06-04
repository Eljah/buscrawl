import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

public class BusStopLastPassAggregationJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STOP_VISITS_DIR = "stop-visit-events";
    private static final String DAILY_ROUTE_STOP_DIR = "daily-route-stop";
    private static final String DAILY_ROUTE_DIR = "daily-route";
    private static final String DAILY_STOP_DIR = "daily-stop";
    private static final String SUMMARY_ROUTE_STOP_ALL_DIR = "summary-route-stop-all-days";
    private static final String SUMMARY_ROUTE_STOP_WEEKDAY_DIR = "summary-route-stop-by-weekday";
    private static final String SUMMARY_ROUTE_ALL_DIR = "summary-route-all-days";
    private static final String SUMMARY_ROUTE_WEEKDAY_DIR = "summary-route-by-weekday";
    private static final String SUMMARY_STOP_ALL_DIR = "summary-stop-all-days";
    private static final String SUMMARY_STOP_WEEKDAY_DIR = "summary-stop-by-weekday";

    public static void main(String[] args) throws Exception {
        Path parquetDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path outputRoot = Path.of(System.getenv().getOrDefault("BUS_STOP_LAST_PASS_DIR", "./var/bus/stop-last-pass"));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_STOP_LAST_PASS_STATE_FILE",
                outputRoot.resolve("aggregation-state.json").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault("BUS_STOP_LAST_PASS_SPARK_LOCAL_DIR", "./var/bus/stop-last-pass-spark-temp"));
        String sparkMaster = System.getenv().getOrDefault("BUS_STOP_LAST_PASS_SPARK_MASTER", "local[2]");
        String cityTimezone = System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow");
        double stopRadiusMeters = Double.parseDouble(System.getenv().getOrDefault("BUS_STOP_LAST_PASS_RADIUS_METERS", "50"));
        int visitGapSeconds = Integer.parseInt(System.getenv().getOrDefault("BUS_STOP_VISIT_MAX_GAP_SECONDS", "180"));
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_STOP_LAST_PASS_OUTPUT_PARTITIONS", "32"));

        Files.createDirectories(outputRoot);
        Files.createDirectories(sparkLocalDir);
        Files.createDirectories(stateFile.getParent());

        if (!Files.exists(parquetDir)) {
            throw new IllegalStateException("Parquet directory not found: " + parquetDir);
        }

        AggregationState state = loadState(stateFile);
        Instant modifiedSince = state.lastProcessedModifiedAt == null
                ? Instant.EPOCH
                : IncrementalParquetSupport.parseInstant(state.lastProcessedModifiedAt);
        if (modifiedSince == null) {
            modifiedSince = Instant.EPOCH;
        }

        List<IncrementalParquetSupport.ParquetFileInfo> candidateFiles =
                IncrementalParquetSupport.listRecentParquetFiles(parquetDir, modifiedSince);
        List<IncrementalParquetSupport.ParquetFileInfo> newFiles =
                IncrementalParquetSupport.selectNewFiles(candidateFiles, state.lastProcessedModifiedAt, state.lastProcessedPath);

        if (newFiles.isEmpty()) {
            System.out.println("BusStopLastPassAggregationJob: no new parquet files to process");
            return;
        }

        Path stopVisitsDir = outputRoot.resolve(STOP_VISITS_DIR);
        Path dailyRouteStopDir = outputRoot.resolve(DAILY_ROUTE_STOP_DIR);
        Path dailyRouteDir = outputRoot.resolve(DAILY_ROUTE_DIR);
        Path dailyStopDir = outputRoot.resolve(DAILY_STOP_DIR);

        SparkSession spark = SparkSession.builder()
                .appName("BusStopLastPassAggregationJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_STOP_LAST_PASS_DRIVER_MEMORY", "4g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_STOP_LAST_PASS_EXECUTOR_MEMORY", "4g"))
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");
        spark.udf().register("haversineMeters", (UDF4<Double, Double, Double, Double, Double>)
                BusStopLastPassAggregationJob::haversineMeters, DataTypes.DoubleType);

        RouteTopology topology = RouteTopology.load(null);

        try {
            Dataset<Row> routeMetadata = topology.createRouteMetadataDataFrame(spark);
            Dataset<Row> routeStopPoints = topology.createRouteStopPointsDataFrame(spark);

            Dataset<Row> newBusData = spark.read()
                    .parquet(newFiles.stream().map(file -> file.path).toArray(String[]::new))
                    .select("plate", "internalRouteId", "latitude", "longitude", "eventTime")
                    .withColumn("sourceFile", input_file_name())
                    .filter(col("plate").isNotNull())
                    .filter(col("internalRouteId").isNotNull())
                    .filter(col("latitude").isNotNull())
                    .filter(col("longitude").isNotNull())
                    .filter(col("eventTime").isNotNull())
                    .join(routeMetadata, "internalRouteId");

            Dataset<Row> stopMatches = newBusData.join(routeStopPoints, new String[]{"internalRouteId", "routeNumber"})
                    .withColumn("distanceMeters", callUDF(
                            "haversineMeters",
                            col("latitude"),
                            col("longitude"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    ))
                    .filter(col("distanceMeters").leq(stopRadiusMeters))
                    .withColumn("localEventTime", from_utc_timestamp(col("eventTime"), cityTimezone))
                    .withColumn("serviceDate", to_date(col("localEventTime")))
                    .withColumn("weekdayIso", expr("((dayofweek(localEventTime) + 5) % 7) + 1"));

            WindowSpec visitWindow = Window.partitionBy(
                            "serviceDate",
                            "plate",
                            "internalRouteId",
                            "routeNumber",
                            "stopId",
                            "direction"
                    )
                    .orderBy(col("eventTime"));

            Dataset<Row> newStopVisits = stopMatches
                    .withColumn("previousEventTime", lag(col("eventTime"), 1).over(visitWindow))
                    .withColumn("isNewVisit", when(
                            col("previousEventTime").isNull()
                                    .or(expr("unix_timestamp(eventTime) - unix_timestamp(previousEventTime) > " + visitGapSeconds)),
                            1
                    ).otherwise(0))
                    .withColumn("visitSequence", sum(col("isNewVisit")).over(visitWindow))
                    .groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("plate"),
                            col("internalRouteId"),
                            col("routeNumber"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude"),
                            col("direction"),
                            col("visitSequence")
                    )
                    .agg(
                            min("eventTime").alias("enteredStopAt"),
                            max("eventTime").alias("exitedStopAt"),
                            count("*").alias("pointCount"),
                            min("distanceMeters").alias("minDistanceMeters"),
                            max("distanceMeters").alias("maxDistanceMeters"),
                            min("sourceFile").alias("sourceFile")
                    )
                    .withColumn("dwellTimeSeconds", expr("unix_timestamp(exitedStopAt) - unix_timestamp(enteredStopAt)"))
                    .withColumn("enteredStopLocalTime", from_utc_timestamp(col("enteredStopAt"), cityTimezone))
                    .withColumn("exitedStopLocalTime", from_utc_timestamp(col("exitedStopAt"), cityTimezone))
                    .withColumn("enteredStopSeconds", expr("hour(enteredStopLocalTime) * 3600 + minute(enteredStopLocalTime) * 60 + second(enteredStopLocalTime)"))
                    .withColumn("exitedStopSeconds", expr("hour(exitedStopLocalTime) * 3600 + minute(exitedStopLocalTime) * 60 + second(exitedStopLocalTime)"))
                    .withColumn("visitId", expr("sha2(concat_ws('|', cast(serviceDate as string), plate, internalRouteId, routeNumber, stopId, cast(direction as string), cast(enteredStopAt as string), cast(exitedStopAt as string)), 256)"));

            List<String> affectedDates = newStopVisits
                    .selectExpr("CAST(serviceDate AS STRING) AS serviceDate")
                    .distinct()
                    .sort("serviceDate")
                    .as(org.apache.spark.sql.Encoders.STRING())
                    .collectAsList();

            if (affectedDates.isEmpty()) {
                updateState(stateFile, state, newFiles);
                return;
            }

            Dataset<Row> affectedStopVisits = newStopVisits;
            if (hasParquetFiles(stopVisitsDir)) {
                Dataset<Row> existingAffectedStopVisits = spark.read()
                        .parquet(stopVisitsDir.toAbsolutePath().toString())
                        .filter(col("serviceDate").isin(affectedDates.toArray()));
                affectedStopVisits = existingAffectedStopVisits
                        .unionByName(newStopVisits)
                        .dropDuplicates("visitId");
            }

            overwriteAffectedPartitions(affectedStopVisits, stopVisitsDir, outputPartitions, "serviceDate");

            Dataset<Row> dailyRouteStop = affectedStopVisits.groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("routeNumber"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            max("exitedStopAt").alias("lastPassAt"),
                            expr("max_by(enteredStopAt, exitedStopAt)").alias("lastEnteredStopAt"),
                            expr("max_by(dwellTimeSeconds, exitedStopAt)").alias("lastDwellTimeSeconds")
                    )
                    .withColumn("lastPassLocalTime", from_utc_timestamp(col("lastPassAt"), cityTimezone))
                    .withColumn("lastPassSeconds", expr("hour(lastPassLocalTime) * 3600 + minute(lastPassLocalTime) * 60 + second(lastPassLocalTime)"))
                    .drop("lastPassLocalTime");

            Dataset<Row> dailyRoute = dailyRouteStop.groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("routeNumber")
                    )
                    .agg(max("lastPassAt").alias("lastPassAt"))
                    .withColumn("lastPassLocalTime", from_utc_timestamp(col("lastPassAt"), cityTimezone))
                    .withColumn("lastPassSeconds", expr("hour(lastPassLocalTime) * 3600 + minute(lastPassLocalTime) * 60 + second(lastPassLocalTime)"))
                    .drop("lastPassLocalTime");

            Dataset<Row> dailyStop = dailyRouteStop.groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            max("lastPassAt").alias("lastPassAt"),
                            expr("max_by(routeNumber, lastPassAt)").alias("lastRouteNumber")
                    )
                    .withColumn("lastPassLocalTime", from_utc_timestamp(col("lastPassAt"), cityTimezone))
                    .withColumn("lastPassSeconds", expr("hour(lastPassLocalTime) * 3600 + minute(lastPassLocalTime) * 60 + second(lastPassLocalTime)"))
                    .drop("lastPassLocalTime");

            overwriteAffectedPartitions(dailyRouteStop, dailyRouteStopDir, outputPartitions, "serviceDate");
            overwriteAffectedPartitions(dailyRoute, dailyRouteDir, outputPartitions, "serviceDate");
            overwriteAffectedPartitions(dailyStop, dailyStopDir, outputPartitions, "serviceDate");

            Dataset<Row> fullDailyRouteStop = spark.read().parquet(dailyRouteStopDir.toAbsolutePath().toString());
            Dataset<Row> fullDailyRoute = spark.read().parquet(dailyRouteDir.toAbsolutePath().toString());
            Dataset<Row> fullDailyStop = spark.read().parquet(dailyStopDir.toAbsolutePath().toString());

            Dataset<Row> summaryRouteStopAll = fullDailyRouteStop.groupBy(
                            col("routeNumber"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            Dataset<Row> summaryRouteStopWeekday = fullDailyRouteStop.groupBy(
                            col("weekdayIso"),
                            col("routeNumber"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            Dataset<Row> summaryRouteAll = fullDailyRoute.groupBy(col("routeNumber"))
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            Dataset<Row> summaryRouteWeekday = fullDailyRoute.groupBy(col("weekdayIso"), col("routeNumber"))
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            Dataset<Row> summaryStopAll = fullDailyStop.groupBy(
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            Dataset<Row> summaryStopWeekday = fullDailyStop.groupBy(
                            col("weekdayIso"),
                            col("stopId"),
                            col("stopName"),
                            col("stopLatitude"),
                            col("stopLongitude")
                    )
                    .agg(
                            round(avg("lastPassSeconds")).cast(DataTypes.IntegerType).alias("averageLastPassSeconds"),
                            count("*").alias("sampleDays")
                    );

            writeDataset(summaryRouteStopAll, outputRoot.resolve(SUMMARY_ROUTE_STOP_ALL_DIR), outputPartitions);
            writeDataset(summaryRouteStopWeekday, outputRoot.resolve(SUMMARY_ROUTE_STOP_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryRouteAll, outputRoot.resolve(SUMMARY_ROUTE_ALL_DIR), outputPartitions);
            writeDataset(summaryRouteWeekday, outputRoot.resolve(SUMMARY_ROUTE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryStopAll, outputRoot.resolve(SUMMARY_STOP_ALL_DIR), outputPartitions);
            writeDataset(summaryStopWeekday, outputRoot.resolve(SUMMARY_STOP_WEEKDAY_DIR), outputPartitions);
        } finally {
            spark.stop();
        }

        updateState(stateFile, state, newFiles);
    }

    private static void overwriteAffectedPartitions(Dataset<Row> dataset, Path outputDir, int partitions, String partitionColumn) {
        dataset.repartition(Math.max(1, partitions), col(partitionColumn))
                .write()
                .mode("overwrite")
                .partitionBy(partitionColumn)
                .parquet(outputDir.toAbsolutePath().toString());
    }

    private static void writeDataset(Dataset<Row> dataset, Path outputDir, int partitions) {
        dataset.coalesce(Math.max(1, partitions))
                .write()
                .mode("overwrite")
                .parquet(outputDir.toAbsolutePath().toString());
    }

    private static boolean hasParquetFiles(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return false;
        }
        try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
            return files.anyMatch(path -> path.getFileName().toString().endsWith(".parquet"));
        }
    }

    private static AggregationState loadState(Path stateFile) throws Exception {
        if (!Files.exists(stateFile)) {
            return new AggregationState();
        }
        return MAPPER.readValue(stateFile.toFile(), AggregationState.class);
    }

    private static void updateState(
            Path stateFile,
            AggregationState state,
            List<IncrementalParquetSupport.ParquetFileInfo> processedFiles
    ) throws Exception {
        if (processedFiles.isEmpty()) {
            return;
        }
        IncrementalParquetSupport.ParquetFileInfo last = processedFiles.get(processedFiles.size() - 1);
        state.updatedAt = Instant.now().atOffset(ZoneOffset.UTC).toString();
        state.lastProcessedModifiedAt = last.modifiedAt;
        state.lastProcessedPath = last.path;

        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double earthRadiusMeters = 6_371_000;
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2)
                * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        return earthRadiusMeters * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    public static final class AggregationState {
        public String updatedAt;
        public String lastProcessedModifiedAt;
        public String lastProcessedPath;
    }
}
