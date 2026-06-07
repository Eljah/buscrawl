import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.max_by;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

public class BusTrafficBehaviorAggregationJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DWELL_EVENTS_DIR = "dwell-events";
    private static final String SEGMENT_TRIPS_DIR = "segment-trips";
    private static final String OVERTAKE_EVENTS_DIR = "overtake-events";
    private static final String PHYSICAL_OVERTAKE_EVENTS_DIR = "physical-overtake-events";
    private static final String DAILY_OVERTAKE_SEGMENT_DIR = "daily-overtake-segment";
    private static final String DAILY_OVERTAKE_SEGMENT_ROUTE_DIR = "daily-overtake-segment-route";
    private static final String DAILY_OVERTAKE_SEGMENT_VEHICLE_DIR = "daily-overtake-segment-vehicle";
    private static final String DAILY_OVERTAKE_VEHICLE_DIR = "daily-overtake-vehicle";
    private static final String DAILY_PHYSICAL_OVERTAKE_SEGMENT_DIR = "daily-physical-overtake-segment";
    private static final String DAILY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_DIR = "daily-physical-overtake-segment-vehicle";
    private static final String DAILY_PHYSICAL_OVERTAKE_VEHICLE_DIR = "daily-physical-overtake-vehicle";
    private static final String SUMMARY_OVERTAKE_ALL_DIR = "summary-overtake-all-days";
    private static final String SUMMARY_OVERTAKE_WEEKDAY_DIR = "summary-overtake-by-weekday";
    private static final String SUMMARY_OVERTAKE_SEGMENT_ROUTE_ALL_DIR = "summary-overtake-segment-route-all-days";
    private static final String SUMMARY_OVERTAKE_SEGMENT_ROUTE_WEEKDAY_DIR = "summary-overtake-segment-route-by-weekday";
    private static final String SUMMARY_OVERTAKE_SEGMENT_VEHICLE_ALL_DIR = "summary-overtake-segment-vehicle-all-days";
    private static final String SUMMARY_OVERTAKE_SEGMENT_VEHICLE_WEEKDAY_DIR = "summary-overtake-segment-vehicle-by-weekday";
    private static final String SUMMARY_OVERTAKE_VEHICLE_ALL_DIR = "summary-overtake-vehicle-all-days";
    private static final String SUMMARY_OVERTAKE_VEHICLE_WEEKDAY_DIR = "summary-overtake-vehicle-by-weekday";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_ALL_DIR = "summary-physical-overtake-segment-all-days";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_WEEKDAY_DIR = "summary-physical-overtake-segment-by-weekday";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_ALL_DIR = "summary-physical-overtake-segment-vehicle-all-days";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_WEEKDAY_DIR = "summary-physical-overtake-segment-vehicle-by-weekday";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_VEHICLE_ALL_DIR = "summary-physical-overtake-vehicle-all-days";
    private static final String SUMMARY_PHYSICAL_OVERTAKE_VEHICLE_WEEKDAY_DIR = "summary-physical-overtake-vehicle-by-weekday";
    private static final String DAILY_RUBBER_STOP_DIR = "daily-rubber-stop";
    private static final String DAILY_RUBBER_STOP_ROUTE_DIR = "daily-rubber-stop-route";
    private static final String DAILY_RUBBER_ROUTE_DIR = "daily-rubber-route";
    private static final String DAILY_RUBBER_VEHICLE_DIR = "daily-rubber-vehicle";
    private static final String SUMMARY_RUBBER_STOP_ALL_DIR = "summary-rubber-stop-all-days";
    private static final String SUMMARY_RUBBER_STOP_WEEKDAY_DIR = "summary-rubber-stop-by-weekday";
    private static final String SUMMARY_RUBBER_STOP_ROUTE_ALL_DIR = "summary-rubber-stop-route-all-days";
    private static final String SUMMARY_RUBBER_STOP_ROUTE_WEEKDAY_DIR = "summary-rubber-stop-route-by-weekday";
    private static final String SUMMARY_RUBBER_ROUTE_ALL_DIR = "summary-rubber-route-all-days";
    private static final String SUMMARY_RUBBER_ROUTE_WEEKDAY_DIR = "summary-rubber-route-by-weekday";
    private static final String SUMMARY_RUBBER_VEHICLE_ALL_DIR = "summary-rubber-vehicle-all-days";
    private static final String SUMMARY_RUBBER_VEHICLE_WEEKDAY_DIR = "summary-rubber-vehicle-by-weekday";

    public static void main(String[] args) throws Exception {
        Path stopVisitsDir = Path.of(System.getenv().getOrDefault(
                "BUS_STOP_VISITS_DIR",
                "./var/bus/stop-last-pass/stop-visit-events"
        ));
        Path outputRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_STATE_FILE",
                outputRoot.resolve("aggregation-state.json").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_SPARK_LOCAL_DIR",
                "./var/bus/traffic-behavior-spark-temp"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_SPARK_MASTER", "local[2]");
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_OUTPUT_PARTITIONS", "32"));
        int maxFilesPerRun = Integer.parseInt(System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_MAX_FILES_PER_RUN", "512"));
        int initialLookbackDays = Integer.parseInt(System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_INITIAL_LOOKBACK_DAYS", "3"));
        String bootstrapStrategy = System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_BOOTSTRAP_MODE", "full-history");
        int maxSegmentGapSeconds = Integer.parseInt(System.getenv().getOrDefault("BUS_SEGMENT_MAX_GAP_SECONDS", "900"));
        double minSegmentSpeedKmh = Double.parseDouble(System.getenv().getOrDefault("BUS_SEGMENT_MIN_SPEED_KMH", "3.0"));
        double maxSegmentSpeedKmh = Double.parseDouble(System.getenv().getOrDefault("BUS_SEGMENT_MAX_SPEED_KMH", "120.0"));
        String cityTimezone = System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow");
        LocalDate today = LocalDate.now(ZoneId.of(cityTimezone));

        Files.createDirectories(outputRoot);
        Files.createDirectories(sparkLocalDir);
        Files.createDirectories(stateFile.getParent());

        if (!Files.exists(stopVisitsDir)) {
            throw new IllegalStateException("Stop visits directory not found: " + stopVisitsDir);
        }

        BusStopLastPassAggregationJob.AggregationState state = loadState(stateFile);
        Instant modifiedSince;
        if (state.lastProcessedModifiedAt == null) {
            modifiedSince = "recent-window".equalsIgnoreCase(bootstrapStrategy)
                    ? Instant.now().minus(Duration.ofDays(initialLookbackDays))
                    : Instant.EPOCH;
        } else {
            modifiedSince = IncrementalParquetSupport.parseInstant(state.lastProcessedModifiedAt);
        }
        if (modifiedSince == null) {
            modifiedSince = Instant.now().minus(Duration.ofDays(initialLookbackDays));
        }

        List<IncrementalParquetSupport.ParquetFileInfo> candidateFiles =
                IncrementalParquetSupport.listRecentParquetFiles(stopVisitsDir, modifiedSince);
        List<IncrementalParquetSupport.ParquetFileInfo> selectedNewFiles =
                IncrementalParquetSupport.selectNewFiles(candidateFiles, state.lastProcessedModifiedAt, state.lastProcessedPath);
        if (selectedNewFiles.isEmpty()) {
            System.out.println("BusTrafficBehaviorAggregationJob: no new stop-visit parquet files to process");
            return;
        }

        int cappedSize = Math.min(selectedNewFiles.size(), Math.max(1, maxFilesPerRun));
        List<IncrementalParquetSupport.ParquetFileInfo> newFiles = new ArrayList<>(selectedNewFiles.subList(0, cappedSize));
        System.out.printf(
                "BusTrafficBehaviorAggregationJob: processing %d files out of %d available since %s%n",
                newFiles.size(),
                selectedNewFiles.size(),
                modifiedSince
        );

        SparkSession spark = SparkSession.builder()
                .appName("BusTrafficBehaviorAggregationJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_DRIVER_MEMORY", "4g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_TRAFFIC_BEHAVIOR_EXECUTOR_MEMORY", "4g"))
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");

        RouteTopology topology = RouteTopology.load(null);

        try {
            Dataset<Row> routeStops = topology.createRouteStopPointsDataFrame(spark);
            Dataset<Row> routeTerminalBounds = routeStops.groupBy("internalRouteId", "routeNumber", "direction")
                    .agg(max("stopOrder").alias("maxStopOrder"));
            Dataset<Row> routeStopContext = routeStops.join(
                    routeTerminalBounds,
                    new String[]{"internalRouteId", "routeNumber", "direction"}
            );
            Dataset<Row> segments = topology.createAdjacentSegmentsDataFrame(spark).alias("segments");

            Dataset<Row> changedVisits = spark.read()
                    .parquet(newFiles.stream().map(file -> file.path).toArray(String[]::new))
                    .withColumn("serviceDate", to_date(col("enteredStopLocalTime")));
            List<String> affectedDates = changedVisits
                    .selectExpr("CAST(serviceDate AS STRING) AS serviceDate")
                    .distinct()
                    .sort("serviceDate")
                    .as(org.apache.spark.sql.Encoders.STRING())
                    .collectAsList();
            if (affectedDates.isEmpty()) {
                updateState(stateFile, state, newFiles);
                return;
            }

            Dataset<Row> affectedVisits = spark.read()
                    .parquet(stopVisitsDir.toAbsolutePath().toString())
                    .withColumn("serviceDate", to_date(col("enteredStopLocalTime")))
                    .filter(col("serviceDate").isin(affectedDates.toArray()))
                    .join(
                            routeStopContext.select(
                                    "internalRouteId",
                                    "routeNumber",
                                    "stopId",
                                    "direction",
                                    "stopOrder",
                                    "maxStopOrder"
                            ),
                            new String[]{"internalRouteId", "routeNumber", "stopId", "direction"}
                    );
            Dataset<Row> ambiguousVisits = affectedVisits
                    .groupBy(
                            "serviceDate",
                            "plate",
                            "internalRouteId",
                            "routeNumber",
                            "stopId",
                            "enteredStopAt",
                            "exitedStopAt"
                    )
                    .agg(countDistinct("direction").alias("matchedDirections"))
                    .filter(col("matchedDirections").gt(1));
            Dataset<Row> directionSafeVisits = affectedVisits.join(
                    ambiguousVisits.select(
                            "serviceDate",
                            "plate",
                            "internalRouteId",
                            "routeNumber",
                            "stopId",
                            "enteredStopAt",
                            "exitedStopAt"
                    ),
                    new String[]{
                            "serviceDate",
                            "plate",
                            "internalRouteId",
                            "routeNumber",
                            "stopId",
                            "enteredStopAt",
                            "exitedStopAt"
                    },
                    "left_anti"
            );

            Dataset<Row> dwellEvents = affectedVisits.select(
                            "visitId",
                            "serviceDate",
                            "weekdayIso",
                            "plate",
                            "internalRouteId",
                            "routeNumber",
                            "direction",
                            "stopId",
                            "stopName",
                            "stopLatitude",
                            "stopLongitude",
                            "stopOrder",
                            "maxStopOrder",
                            "enteredStopAt",
                            "exitedStopAt",
                            "dwellTimeSeconds",
                            "pointCount",
                            "minDistanceMeters",
                            "maxDistanceMeters"
                    )
                    .withColumn("isTerminalStop", col("stopOrder").equalTo(lit(1)).or(col("stopOrder").equalTo(col("maxStopOrder"))))
                    .dropDuplicates("visitId");

            Path dwellEventsDir = outputRoot.resolve(DWELL_EVENTS_DIR);
            overwriteAffectedPartitions(dwellEvents, dwellEventsDir, outputPartitions, "serviceDate");

            Dataset<Row> startVisits = directionSafeVisits.alias("start");
            Dataset<Row> endVisits = directionSafeVisits.alias("finish");
            Dataset<Row> segmentCandidates = startVisits.join(
                            endVisits,
                            col("start.serviceDate").equalTo(col("finish.serviceDate"))
                                    .and(col("start.plate").equalTo(col("finish.plate")))
                                    .and(col("start.internalRouteId").equalTo(col("finish.internalRouteId")))
                                    .and(col("start.routeNumber").equalTo(col("finish.routeNumber")))
                                    .and(col("start.direction").equalTo(col("finish.direction")))
                                    .and(col("finish.stopOrder").equalTo(col("start.stopOrder").plus(1)))
                                    .and(col("finish.enteredStopAt").gt(col("start.exitedStopAt"))),
                            "inner"
                    )
                    .withColumn("travelDurationSeconds", expr("unix_timestamp(finish.enteredStopAt) - unix_timestamp(start.exitedStopAt)"))
                    .filter(col("travelDurationSeconds").gt(0))
                    .filter(col("travelDurationSeconds").leq(maxSegmentGapSeconds));

            WindowSpec candidateWindow = Window.partitionBy(col("start.visitId"))
                    .orderBy(col("finish.enteredStopAt").asc());
            Dataset<Row> affectedSegmentTrips = segmentCandidates
                    .withColumn("candidateRank", row_number().over(candidateWindow))
                    .filter(col("candidateRank").equalTo(1))
                    .join(
                            segments,
                            col("segments.internalRouteId").equalTo(col("start.internalRouteId"))
                                    .and(col("segments.routeNumber").equalTo(col("start.routeNumber")))
                                    .and(col("segments.direction").equalTo(col("start.direction")))
                                    .and(col("segments.startStopId").equalTo(col("start.stopId")))
                                    .and(col("segments.endStopId").equalTo(col("finish.stopId"))),
                            "inner"
                    )
                    .select(
                            col("segments.segmentId").alias("segmentId"),
                            col("start.serviceDate").alias("serviceDate"),
                            col("start.weekdayIso").alias("weekdayIso"),
                            col("start.plate").alias("plate"),
                            col("start.internalRouteId").alias("internalRouteId"),
                            col("start.routeNumber").alias("routeNumber"),
                            col("start.direction").alias("direction"),
                            col("segments.startStopOrder").alias("startStopOrder"),
                            col("segments.endStopOrder").alias("endStopOrder"),
                            col("segments.startStopId").alias("startStopId"),
                            col("segments.startStopName").alias("startStopName"),
                            col("segments.startStopLatitude").alias("startStopLatitude"),
                            col("segments.startStopLongitude").alias("startStopLongitude"),
                            col("segments.endStopId").alias("endStopId"),
                            col("segments.endStopName").alias("endStopName"),
                            col("segments.endStopLatitude").alias("endStopLatitude"),
                            col("segments.endStopLongitude").alias("endStopLongitude"),
                            col("segments.distanceMeters").alias("distanceMeters"),
                            col("start.visitId").alias("startVisitId"),
                            col("finish.visitId").alias("endVisitId"),
                            col("start.exitedStopAt").alias("startExitedStopAt"),
                            col("finish.enteredStopAt").alias("endEnteredStopAt"),
                            col("travelDurationSeconds")
                    )
                    .withColumn(
                            "avgSegmentSpeedKmh",
                            expr("(distanceMeters / travelDurationSeconds) * 3.6")
                    )
                    .withColumn("physicalSegmentId", expr("concat_ws('|', startStopId, endStopId)"))
                    .filter(col("avgSegmentSpeedKmh").geq(minSegmentSpeedKmh))
                    .filter(col("avgSegmentSpeedKmh").leq(maxSegmentSpeedKmh))
                    .withColumn(
                            "tripId",
                            sha2(
                                    expr("concat_ws('|', cast(serviceDate as string), plate, segmentId, cast(startExitedStopAt as string), cast(endEnteredStopAt as string))"),
                                    256
                            )
                    )
                    .dropDuplicates("tripId");

            Path segmentTripsDir = outputRoot.resolve(SEGMENT_TRIPS_DIR);
            overwriteAffectedPartitions(affectedSegmentTrips, segmentTripsDir, outputPartitions, "serviceDate");

            Dataset<Row> overtakeEvents = affectedSegmentTrips.alias("earlier").join(
                            affectedSegmentTrips.alias("later"),
                            col("earlier.serviceDate").equalTo(col("later.serviceDate"))
                                    .and(col("earlier.segmentId").equalTo(col("later.segmentId")))
                                    .and(col("earlier.plate").notEqual(col("later.plate")))
                                    .and(col("earlier.startExitedStopAt").lt(col("later.startExitedStopAt")))
                                    .and(col("earlier.endEnteredStopAt").gt(col("later.endEnteredStopAt"))),
                            "inner"
                    )
                    .select(
                            col("earlier.serviceDate").alias("serviceDate"),
                            col("earlier.weekdayIso").alias("weekdayIso"),
                            col("earlier.segmentId").alias("segmentId"),
                            col("earlier.internalRouteId").alias("internalRouteId"),
                            col("earlier.routeNumber").alias("routeNumber"),
                            col("earlier.direction").alias("direction"),
                            col("earlier.startStopId").alias("startStopId"),
                            col("earlier.startStopName").alias("startStopName"),
                            col("earlier.startStopLatitude").alias("startStopLatitude"),
                            col("earlier.startStopLongitude").alias("startStopLongitude"),
                            col("earlier.endStopId").alias("endStopId"),
                            col("earlier.endStopName").alias("endStopName"),
                            col("earlier.endStopLatitude").alias("endStopLatitude"),
                            col("earlier.endStopLongitude").alias("endStopLongitude"),
                            col("earlier.distanceMeters").alias("distanceMeters"),
                            col("later.plate").alias("overtakerPlate"),
                            col("earlier.plate").alias("overtakenPlate"),
                            col("later.tripId").alias("overtakerTripId"),
                            col("earlier.tripId").alias("overtakenTripId"),
                            col("later.startExitedStopAt").alias("overtakerStartExitedStopAt"),
                            col("later.endEnteredStopAt").alias("overtakerEndEnteredStopAt"),
                            col("later.travelDurationSeconds").alias("overtakerTravelDurationSeconds"),
                            col("later.avgSegmentSpeedKmh").alias("overtakerAvgSegmentSpeedKmh"),
                            col("earlier.startExitedStopAt").alias("overtakenStartExitedStopAt"),
                            col("earlier.endEnteredStopAt").alias("overtakenEndEnteredStopAt"),
                            col("earlier.travelDurationSeconds").alias("overtakenTravelDurationSeconds"),
                            col("earlier.avgSegmentSpeedKmh").alias("overtakenAvgSegmentSpeedKmh")
                    )
                    .withColumn("overtakeAt", col("overtakerEndEnteredStopAt"))
                    .withColumn("overtakerRouteNumber", col("routeNumber"))
                    .withColumn(
                            "overtakeEventId",
                            sha2(
                                    expr("concat_ws('|', cast(serviceDate as string), segmentId, overtakerTripId, overtakenTripId)"),
                                    256
                            )
                    )
                    .dropDuplicates("overtakeEventId");

            Path overtakeEventsDir = outputRoot.resolve(OVERTAKE_EVENTS_DIR);
            overwriteAffectedPartitions(overtakeEvents, overtakeEventsDir, outputPartitions, "serviceDate");

            Dataset<Row> physicalOvertakeEvents = affectedSegmentTrips.alias("earlier").join(
                            affectedSegmentTrips.alias("later"),
                            col("earlier.serviceDate").equalTo(col("later.serviceDate"))
                                    .and(col("earlier.physicalSegmentId").equalTo(col("later.physicalSegmentId")))
                                    .and(col("earlier.plate").notEqual(col("later.plate")))
                                    .and(col("earlier.startExitedStopAt").lt(col("later.startExitedStopAt")))
                                    .and(col("earlier.endEnteredStopAt").gt(col("later.endEnteredStopAt"))),
                            "inner"
                    )
                    .select(
                            col("earlier.serviceDate").alias("serviceDate"),
                            col("earlier.weekdayIso").alias("weekdayIso"),
                            col("earlier.physicalSegmentId").alias("physicalSegmentId"),
                            col("earlier.startStopId").alias("startStopId"),
                            col("earlier.startStopName").alias("startStopName"),
                            col("earlier.startStopLatitude").alias("startStopLatitude"),
                            col("earlier.startStopLongitude").alias("startStopLongitude"),
                            col("earlier.endStopId").alias("endStopId"),
                            col("earlier.endStopName").alias("endStopName"),
                            col("earlier.endStopLatitude").alias("endStopLatitude"),
                            col("earlier.endStopLongitude").alias("endStopLongitude"),
                            col("earlier.distanceMeters").alias("distanceMeters"),
                            col("later.internalRouteId").alias("overtakerInternalRouteId"),
                            col("later.routeNumber").alias("overtakerRouteNumber"),
                            col("later.plate").alias("overtakerPlate"),
                            col("earlier.internalRouteId").alias("overtakenInternalRouteId"),
                            col("earlier.routeNumber").alias("overtakenRouteNumber"),
                            col("earlier.plate").alias("overtakenPlate"),
                            col("later.segmentId").alias("overtakerRouteSegmentId"),
                            col("earlier.segmentId").alias("overtakenRouteSegmentId"),
                            col("later.tripId").alias("overtakerTripId"),
                            col("earlier.tripId").alias("overtakenTripId"),
                            col("later.startExitedStopAt").alias("overtakerStartExitedStopAt"),
                            col("later.endEnteredStopAt").alias("overtakerEndEnteredStopAt"),
                            col("later.travelDurationSeconds").alias("overtakerTravelDurationSeconds"),
                            col("later.avgSegmentSpeedKmh").alias("overtakerAvgSegmentSpeedKmh"),
                            col("earlier.startExitedStopAt").alias("overtakenStartExitedStopAt"),
                            col("earlier.endEnteredStopAt").alias("overtakenEndEnteredStopAt"),
                            col("earlier.travelDurationSeconds").alias("overtakenTravelDurationSeconds"),
                            col("earlier.avgSegmentSpeedKmh").alias("overtakenAvgSegmentSpeedKmh")
                    )
                    .withColumn("overtakeAt", col("overtakerEndEnteredStopAt"))
                    .withColumn(
                            "overtakeEventId",
                            sha2(
                                    expr("concat_ws('|', cast(serviceDate as string), physicalSegmentId, overtakerTripId, overtakenTripId)"),
                                    256
                            )
                    )
                    .dropDuplicates("overtakeEventId");

            Path physicalOvertakeEventsDir = outputRoot.resolve(PHYSICAL_OVERTAKE_EVENTS_DIR);
            overwriteAffectedPartitions(physicalOvertakeEvents, physicalOvertakeEventsDir, outputPartitions, "serviceDate");

            Dataset<Row> dailyOvertakeSegment = overtakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("overtakerPlate"), col("overtakeAt")).alias("latestOvertakerPlate"),
                            max_by(col("overtakerRouteNumber"), col("overtakeAt")).alias("latestOvertakerRouteNumber"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyOvertakeSegment, outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyOvertakeSegmentRoute = overtakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("overtakerPlate"), col("overtakeAt")).alias("latestOvertakerPlate"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyOvertakeSegmentRoute, outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_ROUTE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyOvertakeSegmentVehicle = overtakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyOvertakeSegmentVehicle, outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_VEHICLE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyOvertakeVehicle = overtakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("startStopName"), col("overtakeAt")).alias("latestStartStopName"),
                            max_by(col("endStopName"), col("overtakeAt")).alias("latestEndStopName"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyOvertakeVehicle, outputRoot.resolve(DAILY_OVERTAKE_VEHICLE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyPhysicalOvertakeSegment = physicalOvertakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "physicalSegmentId",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("overtakerPlate"), col("overtakeAt")).alias("latestOvertakerPlate"),
                            max_by(col("overtakerRouteNumber"), col("overtakeAt")).alias("latestOvertakerRouteNumber"),
                            max_by(col("overtakenPlate"), col("overtakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("overtakenRouteNumber"), col("overtakeAt")).alias("latestOvertakenRouteNumber"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyPhysicalOvertakeSegment, outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_SEGMENT_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyPhysicalOvertakeSegmentVehicle = physicalOvertakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "physicalSegmentId",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("overtakenPlate"), col("overtakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("overtakenRouteNumber"), col("overtakeAt")).alias("latestOvertakenRouteNumber"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyPhysicalOvertakeSegmentVehicle, outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dailyPhysicalOvertakeVehicle = physicalOvertakeEvents.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            count("*").alias("overtakeCount"),
                            max("overtakeAt").alias("latestOvertakeAt"),
                            max_by(col("startStopName"), col("overtakeAt")).alias("latestStartStopName"),
                            max_by(col("endStopName"), col("overtakeAt")).alias("latestEndStopName"),
                            round(avg("overtakerTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakerDurationSeconds"),
                            round(avg("overtakenTravelDurationSeconds"), 0).cast(DataTypes.IntegerType).alias("averageOvertakenDurationSeconds")
                    );
            overwriteAffectedPartitions(dailyPhysicalOvertakeVehicle, outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_VEHICLE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> dwellEventsByPolicy = dwellEvents
                    .withColumn("terminalPolicy", lit("with-terminals"))
                    .unionByName(
                            dwellEvents.filter(not(col("isTerminalStop")))
                                    .withColumn("terminalPolicy", lit("without-terminals"))
                    );

            Dataset<Row> dailyRubberStop = dwellEventsByPolicy.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "terminalPolicy",
                            "stopId",
                            "stopName",
                            "stopLatitude",
                            "stopLongitude"
                    )
                    .agg(
                            sum("dwellTimeSeconds").alias("totalDwellSeconds"),
                            max("dwellTimeSeconds").alias("maxDwellSeconds"),
                            round(avg("dwellTimeSeconds"), 0).cast(DataTypes.IntegerType).alias("averageDwellSeconds"),
                            count("*").alias("visitCount")
                    );
            Dataset<Row> dailyRubberStopRoute = dwellEventsByPolicy.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "terminalPolicy",
                            "stopId",
                            "stopName",
                            "stopLatitude",
                            "stopLongitude",
                            "routeNumber"
                    )
                    .agg(
                            sum("dwellTimeSeconds").alias("totalDwellSeconds"),
                            max("dwellTimeSeconds").alias("maxDwellSeconds"),
                            round(avg("dwellTimeSeconds"), 0).cast(DataTypes.IntegerType).alias("averageDwellSeconds"),
                            count("*").alias("visitCount")
                    );
            Dataset<Row> dailyRubberRoute = dwellEventsByPolicy.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "terminalPolicy",
                            "routeNumber"
                    )
                    .agg(
                            sum("dwellTimeSeconds").alias("totalDwellSeconds"),
                            max("dwellTimeSeconds").alias("maxDwellSeconds"),
                            round(avg("dwellTimeSeconds"), 0).cast(DataTypes.IntegerType).alias("averageDwellSeconds"),
                            count("*").alias("visitCount")
                    );
            Dataset<Row> dailyRubberVehicle = dwellEventsByPolicy.groupBy(
                            "serviceDate",
                            "weekdayIso",
                            "terminalPolicy",
                            "plate",
                            "routeNumber"
                    )
                    .agg(
                            sum("dwellTimeSeconds").alias("totalDwellSeconds"),
                            max("dwellTimeSeconds").alias("maxDwellSeconds"),
                            round(avg("dwellTimeSeconds"), 0).cast(DataTypes.IntegerType).alias("averageDwellSeconds"),
                            count("*").alias("visitCount")
                    );
            overwriteAffectedPartitions(dailyRubberStop, outputRoot.resolve(DAILY_RUBBER_STOP_DIR), outputPartitions, "serviceDate");
            overwriteAffectedPartitions(dailyRubberStopRoute, outputRoot.resolve(DAILY_RUBBER_STOP_ROUTE_DIR), outputPartitions, "serviceDate");
            overwriteAffectedPartitions(dailyRubberRoute, outputRoot.resolve(DAILY_RUBBER_ROUTE_DIR), outputPartitions, "serviceDate");
            overwriteAffectedPartitions(dailyRubberVehicle, outputRoot.resolve(DAILY_RUBBER_VEHICLE_DIR), outputPartitions, "serviceDate");

            Dataset<Row> fullDailyOvertakes = spark.read()
                    .parquet(outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyOvertakesSegmentRoute = spark.read()
                    .parquet(outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_ROUTE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyOvertakesSegmentVehicle = spark.read()
                    .parquet(outputRoot.resolve(DAILY_OVERTAKE_SEGMENT_VEHICLE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyOvertakesVehicle = spark.read()
                    .parquet(outputRoot.resolve(DAILY_OVERTAKE_VEHICLE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyPhysicalOvertakesSegment = spark.read()
                    .parquet(outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_SEGMENT_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyPhysicalOvertakesSegmentVehicle = spark.read()
                    .parquet(outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyPhysicalOvertakesVehicle = spark.read()
                    .parquet(outputRoot.resolve(DAILY_PHYSICAL_OVERTAKE_VEHICLE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyRubberStop = spark.read()
                    .parquet(outputRoot.resolve(DAILY_RUBBER_STOP_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyRubberStopRoute = spark.read()
                    .parquet(outputRoot.resolve(DAILY_RUBBER_STOP_ROUTE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyRubberRoute = spark.read()
                    .parquet(outputRoot.resolve(DAILY_RUBBER_ROUTE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));
            Dataset<Row> fullDailyRubberVehicle = spark.read()
                    .parquet(outputRoot.resolve(DAILY_RUBBER_VEHICLE_DIR).toAbsolutePath().toString())
                    .filter(col("serviceDate").lt(expr("DATE '" + today + "'")));

            Dataset<Row> summaryOvertakeAll = fullDailyOvertakes.groupBy(
                            "segmentId",
                            "routeNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt")
                    );
            Dataset<Row> summaryOvertakeWeekday = fullDailyOvertakes.groupBy(
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt")
                    );
            Dataset<Row> summaryOvertakeSegmentRouteAll = fullDailyOvertakesSegmentRoute.groupBy(
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakerPlate"), col("latestOvertakeAt")).alias("latestOvertakerPlate")
                    );
            Dataset<Row> summaryOvertakeSegmentRouteWeekday = fullDailyOvertakesSegmentRoute.groupBy(
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakerPlate"), col("latestOvertakeAt")).alias("latestOvertakerPlate")
                    );
            Dataset<Row> summaryOvertakeSegmentVehicleAll = fullDailyOvertakesSegmentVehicle.groupBy(
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt")
                    );
            Dataset<Row> summaryOvertakeSegmentVehicleWeekday = fullDailyOvertakesSegmentVehicle.groupBy(
                            "weekdayIso",
                            "segmentId",
                            "routeNumber",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt")
                    );
            Dataset<Row> summaryOvertakeVehicleAll = fullDailyOvertakesVehicle.groupBy(
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestStartStopName"), col("latestOvertakeAt")).alias("latestStartStopName"),
                            max_by(col("latestEndStopName"), col("latestOvertakeAt")).alias("latestEndStopName")
                    );
            Dataset<Row> summaryOvertakeVehicleWeekday = fullDailyOvertakesVehicle.groupBy(
                            "weekdayIso",
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestStartStopName"), col("latestOvertakeAt")).alias("latestStartStopName"),
                            max_by(col("latestEndStopName"), col("latestOvertakeAt")).alias("latestEndStopName")
                    );
            Dataset<Row> summaryPhysicalOvertakeSegmentAll = fullDailyPhysicalOvertakesSegment.groupBy(
                            "physicalSegmentId",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakerPlate"), col("latestOvertakeAt")).alias("latestOvertakerPlate"),
                            max_by(col("latestOvertakerRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakerRouteNumber"),
                            max_by(col("latestOvertakenPlate"), col("latestOvertakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("latestOvertakenRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakenRouteNumber")
                    );
            Dataset<Row> summaryPhysicalOvertakeSegmentWeekday = fullDailyPhysicalOvertakesSegment.groupBy(
                            "weekdayIso",
                            "physicalSegmentId",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakerPlate"), col("latestOvertakeAt")).alias("latestOvertakerPlate"),
                            max_by(col("latestOvertakerRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakerRouteNumber"),
                            max_by(col("latestOvertakenPlate"), col("latestOvertakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("latestOvertakenRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakenRouteNumber")
                    );
            Dataset<Row> summaryPhysicalOvertakeSegmentVehicleAll = fullDailyPhysicalOvertakesSegmentVehicle.groupBy(
                            "physicalSegmentId",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakenPlate"), col("latestOvertakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("latestOvertakenRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakenRouteNumber")
                    );
            Dataset<Row> summaryPhysicalOvertakeSegmentVehicleWeekday = fullDailyPhysicalOvertakesSegmentVehicle.groupBy(
                            "weekdayIso",
                            "physicalSegmentId",
                            "overtakerRouteNumber",
                            "overtakerPlate",
                            "startStopId",
                            "startStopName",
                            "startStopLatitude",
                            "startStopLongitude",
                            "endStopId",
                            "endStopName",
                            "endStopLatitude",
                            "endStopLongitude",
                            "distanceMeters"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestOvertakenPlate"), col("latestOvertakeAt")).alias("latestOvertakenPlate"),
                            max_by(col("latestOvertakenRouteNumber"), col("latestOvertakeAt")).alias("latestOvertakenRouteNumber")
                    );
            Dataset<Row> summaryPhysicalOvertakeVehicleAll = fullDailyPhysicalOvertakesVehicle.groupBy(
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestStartStopName"), col("latestOvertakeAt")).alias("latestStartStopName"),
                            max_by(col("latestEndStopName"), col("latestOvertakeAt")).alias("latestEndStopName")
                    );
            Dataset<Row> summaryPhysicalOvertakeVehicleWeekday = fullDailyPhysicalOvertakesVehicle.groupBy(
                            "weekdayIso",
                            "overtakerRouteNumber",
                            "overtakerPlate"
                    )
                    .agg(
                            sum("overtakeCount").alias("totalOvertakes"),
                            count("*").alias("sampleDays"),
                            round(avg("overtakeCount"), 2).alias("averageDailyOvertakes"),
                            max("overtakeCount").alias("maxDailyOvertakes"),
                            max("latestOvertakeAt").alias("latestOvertakeAt"),
                            max_by(col("latestStartStopName"), col("latestOvertakeAt")).alias("latestStartStopName"),
                            max_by(col("latestEndStopName"), col("latestOvertakeAt")).alias("latestEndStopName")
                    );
            writeDataset(summaryOvertakeAll, outputRoot.resolve(SUMMARY_OVERTAKE_ALL_DIR), outputPartitions);
            writeDataset(summaryOvertakeWeekday, outputRoot.resolve(SUMMARY_OVERTAKE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryOvertakeSegmentRouteAll, outputRoot.resolve(SUMMARY_OVERTAKE_SEGMENT_ROUTE_ALL_DIR), outputPartitions);
            writeDataset(summaryOvertakeSegmentRouteWeekday, outputRoot.resolve(SUMMARY_OVERTAKE_SEGMENT_ROUTE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryOvertakeSegmentVehicleAll, outputRoot.resolve(SUMMARY_OVERTAKE_SEGMENT_VEHICLE_ALL_DIR), outputPartitions);
            writeDataset(summaryOvertakeSegmentVehicleWeekday, outputRoot.resolve(SUMMARY_OVERTAKE_SEGMENT_VEHICLE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryOvertakeVehicleAll, outputRoot.resolve(SUMMARY_OVERTAKE_VEHICLE_ALL_DIR), outputPartitions);
            writeDataset(summaryOvertakeVehicleWeekday, outputRoot.resolve(SUMMARY_OVERTAKE_VEHICLE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeSegmentAll, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_ALL_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeSegmentWeekday, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeSegmentVehicleAll, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_ALL_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeSegmentVehicleWeekday, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_SEGMENT_VEHICLE_WEEKDAY_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeVehicleAll, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_VEHICLE_ALL_DIR), outputPartitions);
            writeDataset(summaryPhysicalOvertakeVehicleWeekday, outputRoot.resolve(SUMMARY_PHYSICAL_OVERTAKE_VEHICLE_WEEKDAY_DIR), outputPartitions);

            writeDataset(
                    buildRubberSummary(fullDailyRubberStop, new String[]{"terminalPolicy", "stopId", "stopName", "stopLatitude", "stopLongitude"}),
                    outputRoot.resolve(SUMMARY_RUBBER_STOP_ALL_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberStop, new String[]{"weekdayIso", "terminalPolicy", "stopId", "stopName", "stopLatitude", "stopLongitude"}),
                    outputRoot.resolve(SUMMARY_RUBBER_STOP_WEEKDAY_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberStopRoute, new String[]{"terminalPolicy", "stopId", "stopName", "stopLatitude", "stopLongitude", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_STOP_ROUTE_ALL_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberStopRoute, new String[]{"weekdayIso", "terminalPolicy", "stopId", "stopName", "stopLatitude", "stopLongitude", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_STOP_ROUTE_WEEKDAY_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberRoute, new String[]{"terminalPolicy", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_ROUTE_ALL_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberRoute, new String[]{"weekdayIso", "terminalPolicy", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_ROUTE_WEEKDAY_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberVehicle, new String[]{"terminalPolicy", "plate", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_VEHICLE_ALL_DIR),
                    outputPartitions
            );
            writeDataset(
                    buildRubberSummary(fullDailyRubberVehicle, new String[]{"weekdayIso", "terminalPolicy", "plate", "routeNumber"}),
                    outputRoot.resolve(SUMMARY_RUBBER_VEHICLE_WEEKDAY_DIR),
                    outputPartitions
            );
        } finally {
            spark.stop();
        }

        updateState(stateFile, state, newFiles);
    }

    private static Dataset<Row> buildRubberSummary(Dataset<Row> dataset, String[] groupColumns) {
        org.apache.spark.sql.Column[] columns = new org.apache.spark.sql.Column[groupColumns.length];
        for (int i = 0; i < groupColumns.length; i++) {
            columns[i] = col(groupColumns[i]);
        }
        return dataset.groupBy(columns)
                .agg(
                        sum("totalDwellSeconds").alias("totalDwellSeconds"),
                        max("maxDwellSeconds").alias("maxDwellSeconds"),
                        round(avg("averageDwellSeconds"), 0).cast(DataTypes.IntegerType).alias("averageDwellSeconds"),
                        sum("visitCount").alias("visitCount"),
                        countDistinct("serviceDate").alias("sampleDays")
                );
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

    private static BusStopLastPassAggregationJob.AggregationState loadState(Path stateFile) throws Exception {
        if (!Files.exists(stateFile)) {
            return new BusStopLastPassAggregationJob.AggregationState();
        }
        return MAPPER.readValue(stateFile.toFile(), BusStopLastPassAggregationJob.AggregationState.class);
    }

    private static void updateState(
            Path stateFile,
            BusStopLastPassAggregationJob.AggregationState state,
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
}
