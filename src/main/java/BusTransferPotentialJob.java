import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.max_by;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

public class BusTransferPotentialJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String JOURNEYS_DIR = "journeys";
    private static final String FRAGMENTS_DIR = "journey-fragments";
    private static final String REQUEST_COUNTS_DIR = "request-grid-counts";
    private static final ZoneId UTC = ZoneOffset.UTC;

    public static void main(String[] args) throws Exception {
        Path trafficBehaviorDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path segmentTripsDir = Path.of(System.getenv().getOrDefault(
                "BUS_SEGMENT_TRIPS_DIR",
                trafficBehaviorDir.resolve("segment-trips").toString()
        ));
        Path outputRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_POTENTIAL_DIR",
                "./var/bus/transfer-potential"
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_POTENTIAL_STATE_FILE",
                outputRoot.resolve("aggregation-state.json").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_POTENTIAL_SPARK_LOCAL_DIR",
                "./var/bus/transfer-potential-spark-temp"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_TRANSFER_POTENTIAL_SPARK_MASTER", "local[2]");
        String cityTimezone = System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow");
        ZoneId cityZone = ZoneId.of(cityTimezone);
        int bucketMinutes = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_BUCKET_MINUTES", "10"));
        int maxRides = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_RIDES", "10"));
        int maxCandidateEventsPerStop = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_STOP", "1"));
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_POTENTIAL_OUTPUT_PARTITIONS", "24"));
        int maxBucketsPerRun = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_BUCKETS_PER_RUN", "1"));
        long maxDetailedJourneysPerDay = Long.parseLong(System.getenv().getOrDefault(
                "BUS_TRANSFER_MAX_DETAILED_JOURNEYS_PER_DAY",
                "5000000"
        ));
        String targetDateText = System.getenv().getOrDefault("BUS_TRANSFER_TARGET_DATE", "").trim();
        boolean backfill = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_TRANSFER_BACKFILL", "false"));

        Files.createDirectories(outputRoot);
        Files.createDirectories(stateFile.getParent());
        Files.createDirectories(sparkLocalDir);
        if (!Files.exists(segmentTripsDir)) {
            throw new IllegalStateException("Segment trips directory not found: " + segmentTripsDir);
        }

        SparkSession spark = SparkSession.builder()
                .appName("BusTransferPotentialJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_TRANSFER_POTENTIAL_DRIVER_MEMORY", "8g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_TRANSFER_POTENTIAL_EXECUTOR_MEMORY", "8g"))
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");

        try {
            Dataset<Row> allSegmentTrips = spark.read()
                    .parquet(segmentTripsDir.toAbsolutePath().toString())
                    .filter(col("serviceDate").isNotNull())
                    .filter(col("startExitedStopAt").isNotNull())
                    .filter(col("endEnteredStopAt").isNotNull());

            List<LocalDate> serviceDates = selectServiceDates(spark, allSegmentTrips, stateFile, targetDateText, backfill, cityZone);
            if (serviceDates.isEmpty()) {
                System.out.println("BusTransferPotentialJob: no completed service dates to process");
                return;
            }

            RouteTopology topology = RouteTopology.load(null);
            Set<String> topologyStops = collectTopologyStopIds(topology);
            State state = loadState(stateFile);
            List<String> processedBucketKeys = new ArrayList<>();
            int remainingBuckets = Math.max(1, maxBucketsPerRun);
            for (LocalDate serviceDate : serviceDates) {
                ProcessResult result = processServiceDate(
                        spark,
                        allSegmentTrips,
                        outputRoot,
                        serviceDate,
                        cityZone,
                        topologyStops,
                        bucketMinutes,
                        maxRides,
                        maxCandidateEventsPerStop,
                        maxDetailedJourneysPerDay,
                        outputPartitions,
                        state.processedBucketKeys,
                        remainingBuckets
                );
                processedBucketKeys.addAll(result.processedBucketKeys);
                remainingBuckets -= result.processedBucketKeys.size();
                if (remainingBuckets <= 0) {
                    break;
                }
            }
            if (processedBucketKeys.isEmpty()) {
                System.out.println("BusTransferPotentialJob: no unprocessed 10-minute buckets found");
                return;
            }
            rebuildSummaries(spark, outputRoot, outputPartitions);
            updateState(stateFile, processedBucketKeys);
        } finally {
            spark.stop();
        }
    }

    private static List<LocalDate> selectServiceDates(
            SparkSession spark,
            Dataset<Row> allSegmentTrips,
            Path stateFile,
            String targetDateText,
            boolean backfill,
            ZoneId cityZone
    ) throws Exception {
        LocalDate today = LocalDate.now(cityZone);
        LocalDate latestClosedDate = today.minusDays(1);
        if (!targetDateText.isBlank()) {
            return List.of(LocalDate.parse(targetDateText));
        }
        List<String> dateTexts = allSegmentTrips
                .selectExpr("CAST(serviceDate AS STRING) AS serviceDate")
                .distinct()
                .as(org.apache.spark.sql.Encoders.STRING())
                .collectAsList();
        dateTexts.sort(Comparator.reverseOrder());
        List<LocalDate> dates = new ArrayList<>();
        for (String text : dateTexts) {
            if (text == null || text.isBlank()) {
                continue;
            }
            LocalDate date = LocalDate.parse(text);
            if (!date.isAfter(latestClosedDate)) {
                dates.add(date);
            }
        }
        return dates;
    }

    private static ProcessResult processServiceDate(
            SparkSession spark,
            Dataset<Row> allSegmentTrips,
            Path outputRoot,
            LocalDate serviceDate,
            ZoneId cityZone,
            Set<String> topologyStops,
            int bucketMinutes,
            int maxRides,
            int maxCandidateEventsPerStop,
            long maxDetailedJourneysPerDay,
            int outputPartitions,
            Collection<String> alreadyProcessedBucketKeys,
            int maxBucketsToProcess
    ) {
        String serviceDateText = serviceDate.toString();
        Dataset<Row> dayTrips = allSegmentTrips
                .filter(expr("CAST(serviceDate AS STRING) = '" + serviceDateText + "'"))
                .select(
                        "tripId",
                        "serviceDate",
                        "weekdayIso",
                        "plate",
                        "internalRouteId",
                        "routeNumber",
                        "direction",
                        "segmentId",
                        "physicalSegmentId",
                        "startStopId",
                        "startStopName",
                        "endStopId",
                        "endStopName",
                        "distanceMeters",
                        "startExitedStopAt",
                        "endEnteredStopAt",
                        "travelDurationSeconds"
                )
                .filter(col("travelDurationSeconds").gt(0));

        List<Row> rows = dayTrips.collectAsList();
        if (rows.isEmpty()) {
            System.out.println("BusTransferPotentialJob: no segment trips for " + serviceDateText);
            return ProcessResult.empty();
        }

        List<Event> events = new ArrayList<>(rows.size());
        Set<String> stopIds = new TreeSet<>(topologyStops);
        int weekdayIso = 0;
        int minDepartureSecond = Integer.MAX_VALUE;
        int maxDepartureSecond = 0;
        for (Row row : rows) {
            Event event = Event.fromRow(row, cityZone);
            if (event == null) {
                continue;
            }
            events.add(event);
            stopIds.add(event.startStopId);
            stopIds.add(event.endStopId);
            weekdayIso = event.weekdayIso;
            minDepartureSecond = Math.min(minDepartureSecond, event.departureSecond);
            maxDepartureSecond = Math.max(maxDepartureSecond, event.departureSecond);
        }
        if (events.isEmpty()) {
            return ProcessResult.empty();
        }

        Map<String, List<Event>> eventsByStartStop = events.stream()
                .collect(Collectors.groupingBy(event -> event.startStopId));
        for (List<Event> stopEvents : eventsByStartStop.values()) {
            stopEvents.sort(Comparator
                    .comparingInt((Event event) -> event.departureSecond)
                    .thenComparing(event -> event.arrivalSecond)
                    .thenComparing(event -> event.routeNumber)
                    .thenComparing(event -> event.plate));
        }

        List<String> orderedStops = new ArrayList<>(stopIds);
        int firstBucket = floorToBucket(minDepartureSecond, bucketMinutes);
        int lastBucket = floorToBucket(maxDepartureSecond, bucketMinutes);
        List<Row> journeyRows = new ArrayList<>();
        List<Row> fragmentRows = new ArrayList<>();
        List<Row> countRows = new ArrayList<>();
        List<String> processedBucketKeys = new ArrayList<>();
        Set<String> processedBucketKeySet = new HashSet<>(alreadyProcessedBucketKeys);
        long detailedJourneyCount = 0L;
        long possiblePerOrigin = Math.max(0, orderedStops.size() - 1L);

        for (int bucketSecond = firstBucket; bucketSecond <= lastBucket; bucketSecond += bucketMinutes * 60) {
            int bucketMinute = bucketSecond / 60;
            String bucketKey = serviceDateText + "|" + bucketMinute;
            if (processedBucketKeySet.contains(bucketKey)) {
                continue;
            }
            if (processedBucketKeys.size() >= maxBucketsToProcess) {
                break;
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferPotentialJob: processing %s bucketMinute=%d stops=%d events=%d%n",
                    serviceDateText,
                    bucketMinute,
                    orderedStops.size(),
                    events.size()
            );
            Timestamp requestedDepartureAt = Timestamp.from(serviceDate.atStartOfDay(cityZone).plusSeconds(bucketSecond).toInstant());
            long possibleRequests = possiblePerOrigin * orderedStops.size();
            long reachableRequests = 0L;
            long fragmentCount = 0L;

            for (String originStopId : orderedStops) {
                Map<String, StateNode> bestByDestination = findBestJourneys(
                        originStopId,
                        bucketSecond,
                        eventsByStartStop,
                        maxRides,
                        maxCandidateEventsPerStop
                );
                for (Map.Entry<String, StateNode> entry : bestByDestination.entrySet()) {
                    String destinationStopId = entry.getKey();
                    if (originStopId.equals(destinationStopId)) {
                        continue;
                    }
                    StateNode destination = entry.getValue();
                    reachableRequests++;
                    if (detailedJourneyCount >= maxDetailedJourneysPerDay) {
                        continue;
                    }
                    List<Event> path = reconstructPath(destination);
                    if (path.isEmpty()) {
                        continue;
                    }
                    String journeyId = shaKey(serviceDateText + "|" + bucketSecond + "|" + originStopId + "|" + destinationStopId);
                    List<Fragment> fragments = buildFragments(path, bucketSecond);
                    String routePattern = fragments.stream()
                            .map(fragment -> fragment.routeNumber)
                            .collect(Collectors.joining(">"));
                    int totalWaitSeconds = fragments.stream().mapToInt(fragment -> fragment.waitBeforeSeconds).sum();
                    int totalRideSeconds = path.stream().mapToInt(event -> event.travelDurationSeconds).sum();
                    int totalJourneySeconds = destination.arrivalSecond - bucketSecond;
                    Event first = path.get(0);
                    Event last = path.get(path.size() - 1);
                    journeyRows.add(RowFactory.create(
                            journeyId,
                            Date.valueOf(serviceDate),
                            weekdayIso,
                            bucketMinute,
                            requestedDepartureAt,
                            originStopId,
                            first.startStopName,
                            destinationStopId,
                            last.endStopName,
                            true,
                            totalJourneySeconds,
                            totalWaitSeconds,
                            totalRideSeconds,
                            Math.max(0, fragments.size() - 1),
                            fragments.size(),
                            path.size(),
                            routePattern,
                            Timestamp.from(first.departureInstant),
                            Timestamp.from(last.arrivalInstant)
                    ));
                    for (int i = 0; i < fragments.size(); i++) {
                        Fragment fragment = fragments.get(i);
                        fragmentRows.add(RowFactory.create(
                                journeyId,
                                Date.valueOf(serviceDate),
                                weekdayIso,
                                bucketMinute,
                                requestedDepartureAt,
                                i + 1,
                                fragment.internalRouteId,
                                fragment.routeNumber,
                                fragment.plate,
                                fragment.startStopId,
                                fragment.startStopName,
                                fragment.endStopId,
                                fragment.endStopName,
                                fragment.waitBeforeSeconds,
                                fragment.rideSeconds,
                                Timestamp.from(fragment.boardAt),
                                Timestamp.from(fragment.alightAt),
                                fragment.segmentCount,
                                fragment.segmentTripIds
                        ));
                    }
                    fragmentCount += fragments.size();
                    detailedJourneyCount++;
                }
            }
            countRows.add(RowFactory.create(
                    Date.valueOf(serviceDate),
                    weekdayIso,
                    bucketMinute,
                    requestedDepartureAt,
                    possibleRequests,
                    reachableRequests,
                    detailedJourneyCount,
                    fragmentCount,
                    detailedJourneyCount >= maxDetailedJourneysPerDay
            ));
            processedBucketKeys.add(bucketKey);
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferPotentialJob: finished %s bucketMinute=%d reachable=%d detailed=%d fragments=%d truncated=%s%n",
                    serviceDateText,
                    bucketMinute,
                    reachableRequests,
                    detailedJourneyCount,
                    fragmentCount,
                    detailedJourneyCount >= maxDetailedJourneysPerDay
            );
        }

        if (processedBucketKeys.isEmpty()) {
            return ProcessResult.empty();
        }
        writePartitioned(spark.createDataFrame(journeyRows, journeySchema()), outputRoot.resolve(JOURNEYS_DIR), outputPartitions);
        writePartitioned(spark.createDataFrame(fragmentRows, fragmentSchema()), outputRoot.resolve(FRAGMENTS_DIR), outputPartitions);
        writePartitioned(spark.createDataFrame(countRows, requestCountSchema()), outputRoot.resolve(REQUEST_COUNTS_DIR), outputPartitions);
        System.out.printf(
                Locale.ROOT,
                "BusTransferPotentialJob: %s wrote %d detailed journeys, %d fragments, %d request buckets%n",
                serviceDateText,
                journeyRows.size(),
                fragmentRows.size(),
                countRows.size()
        );
        return new ProcessResult(processedBucketKeys);
    }

    private static Map<String, StateNode> findBestJourneys(
            String originStopId,
            int requestedSecond,
            Map<String, List<Event>> eventsByStartStop,
            int maxRides,
            int maxCandidateEventsPerStop
    ) {
        PriorityQueue<StateNode> queue = new PriorityQueue<>(Comparator.comparingInt(node -> node.arrivalSecond));
        Map<String, Integer> bestStateTime = new HashMap<>();
        Map<String, StateNode> bestByDestination = new HashMap<>();
        StateNode start = new StateNode(originStopId, requestedSecond, null, 0, null, null);
        queue.add(start);
        bestStateTime.put(stateKey(start), requestedSecond);

        while (!queue.isEmpty()) {
            StateNode current = queue.poll();
            String currentKey = stateKey(current);
            Integer known = bestStateTime.get(currentKey);
            if (known != null && current.arrivalSecond > known) {
                continue;
            }
            bestByDestination.merge(current.stopId, current, (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right);
            List<Event> events = eventsByStartStop.get(current.stopId);
            if (events == null || events.isEmpty()) {
                continue;
            }
            int startIndex = lowerBoundByDeparture(events, current.arrivalSecond);
            Set<String> usedOutgoingKeys = new HashSet<>();
            int accepted = 0;
            for (int i = startIndex; i < events.size() && accepted < maxCandidateEventsPerStop; i++) {
                Event event = events.get(i);
                String outgoingKey = event.routeNumber + "|" + event.plate + "|" + event.segmentId;
                if (!usedOutgoingKeys.add(outgoingKey)) {
                    continue;
                }
                String rideKey = event.routeNumber + "|" + event.plate;
                int rideCount = rideKey.equals(current.rideKey) ? current.rideCount : current.rideCount + 1;
                if (rideCount > maxRides) {
                    continue;
                }
                StateNode next = new StateNode(event.endStopId, event.arrivalSecond, rideKey, rideCount, current, event);
                String nextKey = stateKey(next);
                Integer previousBest = bestStateTime.get(nextKey);
                if (previousBest == null || next.arrivalSecond < previousBest) {
                    bestStateTime.put(nextKey, next.arrivalSecond);
                    queue.add(next);
                }
                accepted++;
            }
        }
        bestByDestination.remove(originStopId);
        return bestByDestination;
    }

    private static List<Event> reconstructPath(StateNode destination) {
        List<Event> path = new ArrayList<>();
        StateNode current = destination;
        while (current != null && current.event != null) {
            path.add(current.event);
            current = current.previous;
        }
        path.sort(Comparator.comparingInt(event -> event.departureSecond));
        return path;
    }

    private static List<Fragment> buildFragments(List<Event> path, int requestedSecond) {
        List<Fragment> fragments = new ArrayList<>();
        Fragment current = null;
        int readySecond = requestedSecond;
        for (Event event : path) {
            String rideKey = event.routeNumber + "|" + event.plate;
            if (current == null || !current.rideKey.equals(rideKey)) {
                if (current != null) {
                    fragments.add(current);
                }
                current = new Fragment();
                current.rideKey = rideKey;
                current.internalRouteId = event.internalRouteId;
                current.routeNumber = event.routeNumber;
                current.plate = event.plate;
                current.startStopId = event.startStopId;
                current.startStopName = event.startStopName;
                current.endStopId = event.endStopId;
                current.endStopName = event.endStopName;
                current.boardAt = event.departureInstant;
                current.alightAt = event.arrivalInstant;
                current.waitBeforeSeconds = Math.max(0, event.departureSecond - readySecond);
                current.rideSeconds = event.travelDurationSeconds;
                current.segmentCount = 1;
                current.segmentTripIds = event.tripId;
            } else {
                current.endStopId = event.endStopId;
                current.endStopName = event.endStopName;
                current.alightAt = event.arrivalInstant;
                current.rideSeconds += event.travelDurationSeconds;
                current.segmentCount++;
                current.segmentTripIds = current.segmentTripIds + "," + event.tripId;
            }
            readySecond = event.arrivalSecond;
        }
        if (current != null) {
            fragments.add(current);
        }
        return fragments;
    }

    private static void rebuildSummaries(SparkSession spark, Path outputRoot, int outputPartitions) {
        Path journeysDir = outputRoot.resolve(JOURNEYS_DIR);
        if (!Files.exists(journeysDir)) {
            return;
        }
        Dataset<Row> journeys = spark.read().parquet(journeysDir.toAbsolutePath().toString())
                .filter(col("reachable").equalTo(true));
        writeDataset(journeys.groupBy("serviceDate", "weekdayIso", "originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("daily-od-summary"), outputPartitions);
        writeDataset(journeys.groupBy("serviceDate", "weekdayIso", "departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("daily-od-bucket-summary"), outputPartitions);
        writeDataset(journeys.groupBy("originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("summary-od-all-days"), outputPartitions);
        writeDataset(journeys.groupBy("weekdayIso", "originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("summary-od-by-weekday"), outputPartitions);
        writeDataset(journeys.groupBy("departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("summary-od-by-bucket"), outputPartitions);
        writeDataset(journeys.groupBy("weekdayIso", "departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName")
                        .agg(
                                min("totalJourneySeconds").alias("minJourneySeconds"),
                                round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                                count("*").alias("sampleCount"),
                                max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId"),
                                max_by(col("routePattern"), col("routePattern")).alias("sampleRoutePattern")
                        ),
                outputRoot.resolve("summary-od-by-weekday-bucket"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "serviceDate", "weekdayIso", "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("daily-od-route-pattern-summary"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "serviceDate", "weekdayIso", "departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("daily-od-bucket-route-pattern-summary"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("summary-od-route-pattern-all-days"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "weekdayIso", "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("summary-od-route-pattern-by-weekday"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("summary-od-route-pattern-by-bucket"), outputPartitions);
        writeDataset(routePatternSummary(journeys, "weekdayIso", "departureBucketMinute", "originStopId", "originStopName", "destinationStopId", "destinationStopName", "routePattern"),
                outputRoot.resolve("summary-od-route-pattern-by-weekday-bucket"), outputPartitions);
    }

    private static Dataset<Row> routePatternSummary(Dataset<Row> journeys, String... groupColumns) {
        org.apache.spark.sql.Column[] columns = new org.apache.spark.sql.Column[groupColumns.length];
        for (int i = 0; i < groupColumns.length; i++) {
            columns[i] = col(groupColumns[i]);
        }
        return journeys.groupBy(columns)
                .agg(
                        count("*").alias("sampleCount"),
                        round(avg("totalJourneySeconds"), 0).cast(DataTypes.IntegerType).alias("averageJourneySeconds"),
                        round(avg("totalWaitSeconds"), 0).cast(DataTypes.IntegerType).alias("averageWaitSeconds"),
                        round(avg("totalRideSeconds"), 0).cast(DataTypes.IntegerType).alias("averageRideSeconds"),
                        round(avg("transferCount"), 2).alias("averageTransferCount"),
                        min("totalJourneySeconds").alias("minJourneySeconds"),
                        max_by(col("journeyId"), expr("-totalJourneySeconds")).alias("minJourneyId")
                );
    }

    private static Set<String> collectTopologyStopIds(RouteTopology topology) {
        return topology.buildStops().stream()
                .map(stop -> String.valueOf(stop.get("stopId")))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static int floorToBucket(int secondOfDay, int bucketMinutes) {
        int bucketSeconds = bucketMinutes * 60;
        return Math.max(0, (secondOfDay / bucketSeconds) * bucketSeconds);
    }

    private static int lowerBoundByDeparture(List<Event> events, int departureSecond) {
        int left = 0;
        int right = events.size();
        while (left < right) {
            int mid = (left + right) >>> 1;
            if (events.get(mid).departureSecond < departureSecond) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    private static String stateKey(StateNode node) {
        return node.stopId + "|" + node.rideKey + "|" + node.rideCount;
    }

    private static void writePartitioned(Dataset<Row> dataset, Path outputDir, int partitions) {
        dataset.repartition(Math.max(1, partitions), col("serviceDate"))
                .write()
                .mode("overwrite")
                .partitionBy("serviceDate", "departureBucketMinute")
                .parquet(outputDir.toAbsolutePath().toString());
    }

    private static void writeDataset(Dataset<Row> dataset, Path outputDir, int partitions) {
        dataset.coalesce(Math.max(1, partitions))
                .write()
                .mode("overwrite")
                .parquet(outputDir.toAbsolutePath().toString());
    }

    private static State loadState(Path stateFile) throws Exception {
        if (!Files.exists(stateFile)) {
            return new State();
        }
        return MAPPER.readValue(stateFile.toFile(), State.class);
    }

    private static void updateState(Path stateFile, List<String> processedBucketKeys) throws Exception {
        if (processedBucketKeys.isEmpty()) {
            return;
        }
        State state = loadState(stateFile);
        for (String bucketKey : processedBucketKeys) {
            if (!state.processedBucketKeys.contains(bucketKey)) {
                state.processedBucketKeys.add(bucketKey);
            }
            String serviceDate = bucketKey.split("\\|", 2)[0];
            if (!state.processedServiceDates.contains(serviceDate)) {
                state.processedServiceDates.add(serviceDate);
            }
            state.lastProcessedServiceDate = serviceDate;
        }
        state.processedServiceDates.sort(Comparator.naturalOrder());
        state.processedBucketKeys.sort(Comparator.naturalOrder());
        state.updatedAt = Instant.now().atOffset(ZoneOffset.UTC).toString();

        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String shaKey(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to hash journey key", e);
        }
    }

    private static StructType journeySchema() {
        return new StructType(new StructField[]{
                new StructField("journeyId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("serviceDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("weekdayIso", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("departureBucketMinute", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("requestedDepartureAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("originStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("originStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("destinationStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("destinationStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("reachable", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("totalJourneySeconds", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("totalWaitSeconds", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("totalRideSeconds", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("transferCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("rideCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("segmentCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("routePattern", DataTypes.StringType, false, Metadata.empty()),
                new StructField("firstBoardAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("finalAlightAt", DataTypes.TimestampType, false, Metadata.empty())
        });
    }

    private static StructType fragmentSchema() {
        return new StructType(new StructField[]{
                new StructField("journeyId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("serviceDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("weekdayIso", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("departureBucketMinute", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("requestedDepartureAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("fragmentIndex", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("internalRouteId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("routeNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("plate", DataTypes.StringType, false, Metadata.empty()),
                new StructField("startStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("startStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("endStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("endStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("waitBeforeSeconds", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("rideSeconds", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("boardAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("alightAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("segmentCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("segmentTripIds", DataTypes.StringType, false, Metadata.empty())
        });
    }

    private static StructType requestCountSchema() {
        return new StructType(new StructField[]{
                new StructField("serviceDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("weekdayIso", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("departureBucketMinute", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("requestedDepartureAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("possibleRequestCount", DataTypes.LongType, false, Metadata.empty()),
                new StructField("reachableRequestCount", DataTypes.LongType, false, Metadata.empty()),
                new StructField("detailedJourneyCount", DataTypes.LongType, false, Metadata.empty()),
                new StructField("fragmentCount", DataTypes.LongType, false, Metadata.empty()),
                new StructField("detailsTruncated", DataTypes.BooleanType, false, Metadata.empty())
        });
    }

    public static final class State {
        public String updatedAt;
        public String lastProcessedServiceDate;
        public List<String> processedServiceDates = new ArrayList<>();
        public List<String> processedBucketKeys = new ArrayList<>();
    }

    private static final class ProcessResult {
        private final List<String> processedBucketKeys;

        private ProcessResult(List<String> processedBucketKeys) {
            this.processedBucketKeys = processedBucketKeys;
        }

        private static ProcessResult empty() {
            return new ProcessResult(List.of());
        }
    }

    private static final class Event {
        private String tripId;
        private int weekdayIso;
        private String plate;
        private String internalRouteId;
        private String routeNumber;
        private String segmentId;
        private String startStopId;
        private String startStopName;
        private String endStopId;
        private String endStopName;
        private int departureSecond;
        private int arrivalSecond;
        private Instant departureInstant;
        private Instant arrivalInstant;
        private int travelDurationSeconds;

        private static Event fromRow(Row row, ZoneId cityZone) {
            try {
                Event event = new Event();
                event.tripId = row.getAs("tripId");
                event.weekdayIso = ((Number) row.getAs("weekdayIso")).intValue();
                event.plate = row.getAs("plate");
                event.internalRouteId = row.getAs("internalRouteId");
                event.routeNumber = row.getAs("routeNumber");
                event.segmentId = row.getAs("segmentId");
                event.startStopId = row.getAs("startStopId");
                event.startStopName = row.getAs("startStopName");
                event.endStopId = row.getAs("endStopId");
                event.endStopName = row.getAs("endStopName");
                event.departureInstant = rowInstant(row.getAs("startExitedStopAt"));
                event.arrivalInstant = rowInstant(row.getAs("endEnteredStopAt"));
                event.departureSecond = secondOfDay(event.departureInstant, cityZone);
                event.arrivalSecond = secondOfDay(event.arrivalInstant, cityZone);
                event.travelDurationSeconds = ((Number) row.getAs("travelDurationSeconds")).intValue();
                if (event.arrivalSecond <= event.departureSecond || event.travelDurationSeconds <= 0) {
                    return null;
                }
                return event;
            } catch (Exception e) {
                return null;
            }
        }
    }

    private static Instant rowInstant(Object value) {
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(UTC).toInstant();
        }
        if (value instanceof Instant) {
            return (Instant) value;
        }
        return Instant.parse(String.valueOf(value));
    }

    private static int secondOfDay(Instant instant, ZoneId cityZone) {
        LocalTime time = instant.atZone(cityZone).toLocalTime();
        return time.toSecondOfDay();
    }

    private static final class StateNode {
        private final String stopId;
        private final int arrivalSecond;
        private final String rideKey;
        private final int rideCount;
        private final StateNode previous;
        private final Event event;

        private StateNode(String stopId, int arrivalSecond, String rideKey, int rideCount, StateNode previous, Event event) {
            this.stopId = stopId;
            this.arrivalSecond = arrivalSecond;
            this.rideKey = rideKey;
            this.rideCount = rideCount;
            this.previous = previous;
            this.event = event;
        }
    }

    private static final class Fragment {
        private String rideKey;
        private String internalRouteId;
        private String routeNumber;
        private String plate;
        private String startStopId;
        private String startStopName;
        private String endStopId;
        private String endStopName;
        private int waitBeforeSeconds;
        private int rideSeconds;
        private Instant boardAt;
        private Instant alightAt;
        private int segmentCount;
        private String segmentTripIds;
    }
}
