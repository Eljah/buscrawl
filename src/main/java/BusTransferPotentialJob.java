import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
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
import java.util.IdentityHashMap;
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
    private static final int STATIC_GRAPH_BINARY_MAGIC = 0x42534731; // BSG1
    private static final int STATIC_GRAPH_BINARY_VERSION = 1;
    private static final String JOURNEYS_DIR = "journeys";
    private static final String FRAGMENTS_DIR = "journey-fragments";
    private static final String REQUEST_COUNTS_DIR = "request-grid-counts";
    private static final ZoneId UTC = ZoneOffset.UTC;
    private static final int MAX_RIDE_SEGMENT_GAP_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("BUS_TRANSFER_MAX_RIDE_SEGMENT_GAP_SECONDS", "1800")
    );

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
        int maxCandidateEventsPerStop = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_STOP", "120"));
        int maxCandidateEventsPerRoutePattern = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_CANDIDATE_EVENTS_PER_ROUTE_PATTERN", "6"));
        String searchMode = System.getenv().getOrDefault("BUS_TRANSFER_SEARCH_MODE", "route-network").trim();
        boolean staticRouteNetworkFallback = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_ROUTE_NETWORK_FALLBACK",
                "false"
        ));
        int staticFallbackMinDestinations = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_FALLBACK_MIN_DESTINATIONS",
                "0"
        ));
        Path staticGraphCacheDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_GRAPH_DIR",
                "./var/bus/transfer-static-graph"
        ));
        int staticGraphMaxTransfers = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_GRAPH_MAX_TRANSFERS",
                String.valueOf(Math.max(0, Math.min(3, maxRides - 1)))
        ));
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_POTENTIAL_OUTPUT_PARTITIONS", "24"));
        int maxBucketsPerRun = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_MAX_BUCKETS_PER_RUN", "24"));
        String stopBeforeLocalTimeText = System.getenv().getOrDefault("BUS_TRANSFER_STOP_BEFORE_LOCAL_TIME", "04:00").trim();
        long maxDetailedJourneysPerDay = Long.parseLong(System.getenv().getOrDefault(
                "BUS_TRANSFER_MAX_DETAILED_JOURNEYS_PER_DAY",
                "5000000"
        ));
        Set<String> originStopFilter = parseStopIdFilter(System.getenv().getOrDefault(
                "BUS_TRANSFER_ORIGIN_STOP_IDS",
                ""
        ));
        String targetDateText = System.getenv().getOrDefault("BUS_TRANSFER_TARGET_DATE", "").trim();
        boolean backfill = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_TRANSFER_BACKFILL", "false"));
        int writeBatchRows = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_WRITE_BATCH_ROWS", "100000"));
        Instant stopDeadline = computeStopDeadline(cityZone, stopBeforeLocalTimeText);

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
            StaticGraphCache staticGraphCache = StaticGraphCache.empty();
            if ("static-graph-cache".equalsIgnoreCase(searchMode)) {
                staticGraphCache = StaticGraphCache.load(staticGraphCacheDir, staticGraphMaxTransfers);
            }
            State state = loadState(stateFile);
            List<String> processedBucketKeys = new ArrayList<>();
            int remainingBuckets = Math.max(1, maxBucketsPerRun);
            for (LocalDate serviceDate : serviceDates) {
                if (Instant.now().isAfter(stopDeadline)) {
                    System.out.println("BusTransferPotentialJob: stopping before local deadline " + stopBeforeLocalTimeText);
                    break;
                }
                ProcessResult result = processServiceDate(
                        spark,
                        allSegmentTrips,
                        outputRoot,
                        stateFile,
                        serviceDate,
                        cityZone,
                        topologyStops,
                        bucketMinutes,
                        maxRides,
                        maxCandidateEventsPerStop,
                        maxCandidateEventsPerRoutePattern,
                        searchMode,
                        staticGraphCache,
                        staticRouteNetworkFallback,
                        staticFallbackMinDestinations,
                        maxDetailedJourneysPerDay,
                        originStopFilter,
                        outputPartitions,
                        writeBatchRows,
                        state.processedBucketKeys,
                        remainingBuckets,
                        stopDeadline
                );
                processedBucketKeys.addAll(result.processedBucketKeys);
                remainingBuckets -= result.processedBucketKeys.size();
                if (remainingBuckets <= 0) {
                    break;
                }
            }
            if (processedBucketKeys.isEmpty()) {
                System.out.println("BusTransferPotentialJob: no unprocessed 10-minute buckets completed in this run");
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
            Path stateFile,
            LocalDate serviceDate,
            ZoneId cityZone,
            Set<String> topologyStops,
            int bucketMinutes,
            int maxRides,
            int maxCandidateEventsPerStop,
            int maxCandidateEventsPerRoutePattern,
            String searchMode,
            StaticGraphCache staticGraphCache,
            boolean staticRouteNetworkFallback,
            int staticFallbackMinDestinations,
            long maxDetailedJourneysPerDay,
            Set<String> originStopFilter,
            int outputPartitions,
            int writeBatchRows,
            Collection<String> alreadyProcessedBucketKeys,
            int maxBucketsToProcess,
            Instant stopDeadline
    ) throws Exception {
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
                        "startStopOrder",
                        "endStopOrder",
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
        SearchIndex searchIndex = SearchIndex.from(events, eventsByStartStop);
        SearchMode parsedSearchMode = SearchMode.parse(searchMode);

        List<String> orderedStops = new ArrayList<>(stopIds);
        List<String> orderedOriginStops = orderedStops;
        if (!originStopFilter.isEmpty()) {
            orderedOriginStops = orderedStops.stream()
                    .filter(originStopFilter::contains)
                    .collect(Collectors.toList());
            if (orderedOriginStops.isEmpty()) {
                System.out.printf(
                        Locale.ROOT,
                        "BusTransferPotentialJob: no origin stops from BUS_TRANSFER_ORIGIN_STOP_IDS are present for %s%n",
                        serviceDateText
                );
                return ProcessResult.empty();
            }
        }
        int firstBucket = floorToBucket(minDepartureSecond, bucketMinutes);
        int lastBucket = floorToBucket(maxDepartureSecond, bucketMinutes);
        List<String> processedBucketKeys = new ArrayList<>();
        Set<String> processedBucketKeySet = new HashSet<>(alreadyProcessedBucketKeys);
        long detailedJourneyCount = 0L;
        long possiblePerOrigin = Math.max(0, orderedStops.size() - 1L);

        for (int bucketSecond = firstBucket; bucketSecond <= lastBucket; bucketSecond += bucketMinutes * 60) {
            if (Instant.now().isAfter(stopDeadline)) {
                System.out.println("BusTransferPotentialJob: reached stop deadline before next bucket");
                break;
            }
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
                    orderedOriginStops.size(),
                    events.size()
            );
            Timestamp requestedDepartureAt = Timestamp.from(serviceDate.atStartOfDay(cityZone).plusSeconds(bucketSecond).toInstant());
            long possibleRequests = possiblePerOrigin * orderedOriginStops.size();
            long reachableRequests = 0L;
            long fragmentCount = 0L;
            List<Row> journeyRows = new ArrayList<>();
            List<Row> fragmentRows = new ArrayList<>();
            List<Row> countRows = new ArrayList<>();
            clearBucketPartition(outputRoot.resolve(JOURNEYS_DIR), serviceDateText, bucketMinute);
            clearBucketPartition(outputRoot.resolve(FRAGMENTS_DIR), serviceDateText, bucketMinute);
            clearBucketPartition(outputRoot.resolve(REQUEST_COUNTS_DIR), serviceDateText, bucketMinute);

            for (String originStopId : orderedOriginStops) {
                Map<String, StateNode> bestByDestination = findBestJourneys(
                        originStopId,
                        bucketSecond,
                        eventsByStartStop,
                        searchIndex,
                        maxRides,
                        maxCandidateEventsPerStop,
                        maxCandidateEventsPerRoutePattern,
                        parsedSearchMode,
                        staticGraphCache,
                        staticRouteNetworkFallback,
                        staticFallbackMinDestinations
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
                    int totalRideSeconds = fragments.stream().mapToInt(fragment -> fragment.rideSeconds).sum();
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
                    if (journeyRows.size() >= writeBatchRows || fragmentRows.size() >= writeBatchRows) {
                        flushRows(spark, journeyRows, journeySchema(), outputRoot.resolve(JOURNEYS_DIR), outputPartitions);
                        flushRows(spark, fragmentRows, fragmentSchema(), outputRoot.resolve(FRAGMENTS_DIR), outputPartitions);
                    }
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
            flushRows(spark, journeyRows, journeySchema(), outputRoot.resolve(JOURNEYS_DIR), outputPartitions);
            flushRows(spark, fragmentRows, fragmentSchema(), outputRoot.resolve(FRAGMENTS_DIR), outputPartitions);
            flushRows(spark, countRows, requestCountSchema(), outputRoot.resolve(REQUEST_COUNTS_DIR), outputPartitions);
            updateState(stateFile, List.of(bucketKey));
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferPotentialJob: finished and wrote %s bucketMinute=%d reachable=%d detailed=%d fragments=%d truncated=%s%n",
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
        System.out.printf(
                Locale.ROOT,
                "BusTransferPotentialJob: %s completed %d request buckets%n",
                serviceDateText,
                processedBucketKeys.size()
        );
        return new ProcessResult(processedBucketKeys);
    }

    private static Map<String, StateNode> findBestJourneys(
            String originStopId,
            int requestedSecond,
            Map<String, List<Event>> eventsByStartStop,
            SearchIndex searchIndex,
            int maxRides,
            int maxCandidateEventsPerStop,
            int maxCandidateEventsPerRoutePattern,
            SearchMode searchMode,
            StaticGraphCache staticGraphCache,
            boolean staticRouteNetworkFallback,
            int staticFallbackMinDestinations
    ) {
        if (searchMode == SearchMode.STATIC_GRAPH_CACHE) {
            Map<String, StateNode> staticBest = findBestJourneysByStaticGraphCache(
                    originStopId,
                    requestedSecond,
                    eventsByStartStop,
                    searchIndex,
                    staticGraphCache,
                    maxRides,
                    maxCandidateEventsPerRoutePattern
            );
            if (staticRouteNetworkFallback && staticBest.size() < staticFallbackMinDestinations) {
                Map<String, StateNode> fallbackBest = findBestJourneysByRouteNetwork(
                        originStopId,
                        requestedSecond,
                        eventsByStartStop,
                        searchIndex,
                        maxRides,
                        maxCandidateEventsPerRoutePattern
                );
                fallbackBest.forEach((stopId, node) -> staticBest.merge(
                        stopId,
                        node,
                        (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right
                ));
            }
            return staticBest;
        }
        if (searchMode == SearchMode.ROUTE_NETWORK) {
            return findBestJourneysByRouteNetwork(
                    originStopId,
                    requestedSecond,
                    eventsByStartStop,
                    searchIndex,
                    maxRides,
                    maxCandidateEventsPerRoutePattern
            );
        }
        return findBestJourneysLegacy(
                originStopId,
                requestedSecond,
                eventsByStartStop,
                maxRides,
                maxCandidateEventsPerStop
        );
    }

    private static Map<String, StateNode> findBestJourneysByStaticGraphCache(
            String originStopId,
            int requestedSecond,
            Map<String, List<Event>> eventsByStartStop,
            SearchIndex searchIndex,
            StaticGraphCache staticGraphCache,
            int maxRides,
            int maxCandidateEventsPerRoutePattern
    ) {
        Map<String, StateNode> bestByDestination = new HashMap<>();
        List<StaticPathCandidate> candidates = staticGraphCache.candidatesForOrigin(originStopId);
        if (candidates == null || candidates.isEmpty()) {
            return bestByDestination;
        }
        for (StaticPathCandidate candidate : candidates) {
            if (candidate.rideCount > maxRides || candidate.edges.isEmpty()) {
                continue;
            }
            StateNode current = new StateNode(originStopId, requestedSecond, null, 0, null, null);
            int rideCount = 0;
            boolean failed = false;
            for (StaticEdge edge : candidate.edges) {
                if (edge.walk) {
                    int walkSeconds = Math.max(0, (int) Math.ceil(edge.distanceMeters / 1.4));
                    current = new StateNode(edge.toStopId, current.arrivalSecond + walkSeconds, current.rideKey, rideCount, current, null);
                    continue;
                }
                StaticRideEdge rideEdge = (StaticRideEdge) edge;
                StateNode next = followStaticRide(
                        current,
                        rideEdge,
                        eventsByStartStop,
                        searchIndex,
                        rideCount,
                        maxCandidateEventsPerRoutePattern
                );
                if (next == null) {
                    failed = true;
                    break;
                }
                if (next.rideKey == null || !next.rideKey.equals(current.rideKey)) {
                    rideCount++;
                }
                current = new StateNode(next.stopId, next.arrivalSecond, next.rideKey, rideCount, next.previous, next.event);
            }
            if (failed || current.event == null || current.stopId.equals(originStopId)) {
                continue;
            }
            bestByDestination.merge(
                    candidate.destinationStopId,
                    current,
                    (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right
            );
        }
        return bestByDestination;
    }

    private static StateNode followStaticRide(
            StateNode current,
            StaticRideEdge rideEdge,
            Map<String, List<Event>> eventsByStartStop,
            SearchIndex searchIndex,
            int currentRideCount,
            int maxCandidateEventsPerRoutePattern
    ) {
        List<Event> startEvents = eventsByStartStop.get(rideEdge.fromStopId);
        if (startEvents == null || startEvents.isEmpty()) {
            return null;
        }
        int startIndex = lowerBoundByDeparture(startEvents, current.arrivalSecond);
        int accepted = 0;
        Set<String> attemptedBoardEvents = new HashSet<>();
        for (int i = startIndex; i < startEvents.size() && accepted < maxCandidateEventsPerRoutePattern; i++) {
            Event boardEvent = startEvents.get(i);
            if (!rideEdge.matches(boardEvent)) {
                continue;
            }
            String boardKey = boardEvent.rideKey() + "|" + boardEvent.startStopId + "|" + boardEvent.endStopId + "|" + boardEvent.arrivalSecond;
            if (!attemptedBoardEvents.add(boardKey)) {
                continue;
            }
            accepted++;
            StateNode reached = scanStaticRideToStop(current, boardEvent, rideEdge.toStopId, searchIndex, currentRideCount + 1);
            if (reached != null) {
                return reached;
            }
        }
        return null;
    }

    private static StateNode scanStaticRideToStop(
            StateNode current,
            Event boardEvent,
            String targetStopId,
            SearchIndex searchIndex,
            int rideCount
    ) {
        List<Event> rideEvents = searchIndex.eventsByRideKey.get(boardEvent.rideKey());
        Integer index = searchIndex.eventIndexByIdentity.get(boardEvent);
        if (rideEvents == null || index == null) {
            return null;
        }
        StateNode previous = current;
        Event last = null;
        for (int i = index; i < rideEvents.size(); i++) {
            Event event = rideEvents.get(i);
            if (i == index) {
                if (!event.startStopId.equals(boardEvent.startStopId) || event.departureSecond < current.arrivalSecond) {
                    return null;
                }
            } else if (last == null || event.departureSecond < last.arrivalSecond) {
                if (isDuplicateOrOverlappingSegment(event, last)) {
                    continue;
                }
                continue;
            } else if (isNewVehicleRun(event, last)) {
                break;
            } else {
                StateNode skipped = skippedTargetNode(current, previous, last, event, targetStopId, searchIndex, rideCount);
                if (skipped != null) {
                    return skipped;
                }
            }
            StateNode next = new StateNode(event.endStopId, event.arrivalSecond, event.rideKey(), rideCount, previous, event);
            if (event.endStopId.equals(targetStopId)) {
                return next;
            }
            previous = next;
            last = event;
        }
        return null;
    }

    private static Map<String, StateNode> findBestJourneysLegacy(
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

    private static Map<String, StateNode> findBestJourneysByRouteNetwork(
            String originStopId,
            int requestedSecond,
            Map<String, List<Event>> eventsByStartStop,
            SearchIndex searchIndex,
            int maxRides,
            int maxCandidateEventsPerRoutePattern
    ) {
        PriorityQueue<StateNode> queue = new PriorityQueue<>(Comparator.comparingInt(node -> node.arrivalSecond));
        Map<String, int[]> bestStateTime = new HashMap<>();
        Map<String, StateNode> bestByDestination = new HashMap<>();
        StateNode start = new StateNode(originStopId, requestedSecond, null, 0, null, null);
        queue.add(start);
        setBestArrival(bestStateTime, originStopId, 0, requestedSecond, maxRides);

        while (!queue.isEmpty()) {
            StateNode current = queue.poll();
            int known = bestArrival(bestStateTime, current.stopId, current.rideCount);
            if (known != Integer.MAX_VALUE && current.arrivalSecond > known) {
                continue;
            }
            bestByDestination.merge(current.stopId, current, (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right);
            if (current.previous != null && !searchIndex.isTransferStop(current.stopId)) {
                continue;
            }
            List<Event> events = eventsByStartStop.get(current.stopId);
            if (events == null || events.isEmpty()) {
                continue;
            }
            int startIndex = lowerBoundByDeparture(events, current.arrivalSecond);
            Map<String, Integer> acceptedByPattern = new HashMap<>();
            Set<String> usedOutgoingKeys = new HashSet<>();
            int stopPatternCount = searchIndex.routePatternsByStop.getOrDefault(current.stopId, Set.of()).size();
            int fullPatternCount = 0;
            for (int i = startIndex; i < events.size(); i++) {
                if (stopPatternCount > 0 && fullPatternCount >= stopPatternCount) {
                    break;
                }
                Event event = events.get(i);
                String patternKey = event.routePatternKey();
                int accepted = acceptedByPattern.getOrDefault(patternKey, 0);
                if (accepted >= maxCandidateEventsPerRoutePattern) {
                    continue;
                }
                String outgoingKey = event.rideKey() + "|" + event.segmentId;
                if (!usedOutgoingKeys.add(outgoingKey)) {
                    continue;
                }
                int newAccepted = accepted + 1;
                acceptedByPattern.put(patternKey, newAccepted);
                if (newAccepted == maxCandidateEventsPerRoutePattern) {
                    fullPatternCount++;
                }
                int rideCount = event.rideKey().equals(current.rideKey) ? current.rideCount : current.rideCount + 1;
                if (rideCount > maxRides) {
                    continue;
                }
                scanRideForward(current, event, searchIndex, rideCount, maxRides, queue, bestStateTime, bestByDestination);
            }
        }
        bestByDestination.remove(originStopId);
        return bestByDestination;
    }

    private static void scanRideForward(
            StateNode current,
            Event boardEvent,
            SearchIndex searchIndex,
            int rideCount,
            int maxRides,
            PriorityQueue<StateNode> queue,
            Map<String, int[]> bestStateTime,
            Map<String, StateNode> bestByDestination
    ) {
        List<Event> rideEvents = searchIndex.eventsByRideKey.get(boardEvent.rideKey());
        Integer index = searchIndex.eventIndexByIdentity.get(boardEvent);
        if (rideEvents == null || index == null) {
            return;
        }
        StateNode previous = current;
        Event last = null;
        for (int i = index; i < rideEvents.size(); i++) {
            Event event = rideEvents.get(i);
            if (i == index) {
                if (!event.startStopId.equals(current.stopId) || event.departureSecond < current.arrivalSecond) {
                    return;
                }
            } else if (last == null || event.departureSecond < last.arrivalSecond) {
                if (isDuplicateOrOverlappingSegment(event, last)) {
                    continue;
                }
                continue;
            } else if (isNewVehicleRun(event, last)) {
                break;
            }
            StateNode next = new StateNode(event.endStopId, event.arrivalSecond, event.rideKey(), rideCount, previous, event);
            bestByDestination.merge(next.stopId, next, (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right);
            if (searchIndex.isTransferStop(next.stopId)) {
                int previousBest = bestArrival(bestStateTime, next.stopId, rideCount);
                if (next.arrivalSecond < previousBest) {
                    setBestArrival(bestStateTime, next.stopId, rideCount, next.arrivalSecond, maxRides);
                    queue.add(next);
                }
            }
            previous = next;
            last = event;
        }
    }

    private static boolean isDuplicateOrOverlappingSegment(Event event, Event last) {
        return last != null
                && event.segmentId.equals(last.segmentId)
                && event.startStopId.equals(last.startStopId)
                && event.endStopId.equals(last.endStopId)
                && event.arrivalSecond <= last.arrivalSecond;
    }

    private static boolean isNewVehicleRun(Event event, Event last) {
        if (last == null) {
            return false;
        }
        return event.departureSecond - last.arrivalSecond > MAX_RIDE_SEGMENT_GAP_SECONDS;
    }

    private static StateNode skippedTargetNode(
            StateNode current,
            StateNode previous,
            Event last,
            Event nextEvent,
            String targetStopId,
            SearchIndex searchIndex,
            int rideCount
    ) {
        if (last == null
                || !last.routePatternKey().equals(nextEvent.routePatternKey())
                || nextEvent.startStopOrder <= last.endStopOrder + 1) {
            return null;
        }
        StopOnRoute target = searchIndex.stopOnRoute(last.routePatternKey(), targetStopId);
        if (target == null || target.stopOrder <= last.endStopOrder || target.stopOrder >= nextEvent.startStopOrder) {
            return null;
        }
        int gapSeconds = nextEvent.departureSecond - last.arrivalSecond;
        if (gapSeconds <= 0) {
            return null;
        }
        int orderGap = nextEvent.startStopOrder - last.endStopOrder;
        int targetOffset = target.stopOrder - last.endStopOrder;
        int arrivalSecond = last.arrivalSecond + Math.max(1, (int) Math.round(gapSeconds * (targetOffset / (double) orderGap)));
        Event synthetic = Event.syntheticSkippedStop(last, target, arrivalSecond);
        return new StateNode(synthetic.endStopId, synthetic.arrivalSecond, synthetic.rideKey(), rideCount, previous, synthetic);
    }

    private static int bestArrival(Map<String, int[]> bestStateTime, String stopId, int rideCount) {
        int[] arrivals = bestStateTime.get(stopId);
        if (arrivals == null || rideCount < 0 || rideCount >= arrivals.length) {
            return Integer.MAX_VALUE;
        }
        return arrivals[rideCount];
    }

    private static void setBestArrival(Map<String, int[]> bestStateTime, String stopId, int rideCount, int arrivalSecond, int maxRides) {
        int[] arrivals = bestStateTime.computeIfAbsent(stopId, ignored -> {
            int[] values = new int[maxRides + 1];
            java.util.Arrays.fill(values, Integer.MAX_VALUE);
            return values;
        });
        arrivals[rideCount] = arrivalSecond;
    }

    private static List<Event> reconstructPath(StateNode destination) {
        List<Event> path = new ArrayList<>();
        StateNode current = destination;
        while (current != null) {
            if (current.event != null) {
                path.add(current.event);
            }
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
                current.rideSeconds = Math.max(0, event.arrivalSecond - event.departureSecond);
                current.segmentCount = 1;
                current.segmentTripIds = event.tripId;
            } else {
                current.endStopId = event.endStopId;
                current.endStopName = event.endStopName;
                current.alightAt = event.arrivalInstant;
                current.rideSeconds = Math.max(0, (int) java.time.Duration.between(current.boardAt, event.arrivalInstant).getSeconds());
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

    private static Set<String> parseStopIdFilter(String text) {
        if (text == null || text.isBlank()) {
            return Set.of();
        }
        return java.util.Arrays.stream(text.split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static Instant computeStopDeadline(ZoneId cityZone, String stopBeforeLocalTimeText) {
        if (stopBeforeLocalTimeText == null || stopBeforeLocalTimeText.isBlank()) {
            return Instant.MAX;
        }
        LocalTime stopTime = LocalTime.parse(stopBeforeLocalTimeText);
        LocalDate today = LocalDate.now(cityZone);
        return today.atTime(stopTime).atZone(cityZone).toInstant();
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

    private static void flushRows(
            SparkSession spark,
            List<Row> rows,
            StructType schema,
            Path outputDir,
            int partitions
    ) {
        if (rows.isEmpty()) {
            return;
        }
        List<Row> batch = new ArrayList<>(rows);
        rows.clear();
        spark.createDataFrame(batch, schema)
                .repartition(Math.max(1, partitions), col("serviceDate"))
                .write()
                .mode("append")
                .partitionBy("serviceDate", "departureBucketMinute")
                .parquet(outputDir.toAbsolutePath().toString());
    }

    private static void clearBucketPartition(Path outputDir, String serviceDate, int bucketMinute) throws Exception {
        Path partition = outputDir
                .resolve("serviceDate=" + serviceDate)
                .resolve("departureBucketMinute=" + bucketMinute);
        if (Files.exists(partition)) {
            deleteRecursively(partition);
        }
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        List<Path> paths;
        try (java.util.stream.Stream<Path> stream = Files.walk(path)) {
            paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        for (Path item : paths) {
            Files.deleteIfExists(item);
        }
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
        private int direction;
        private String segmentId;
        private int startStopOrder;
        private int endStopOrder;
        private String startStopId;
        private String startStopName;
        private String endStopId;
        private String endStopName;
        private int departureSecond;
        private int arrivalSecond;
        private Instant departureInstant;
        private Instant arrivalInstant;
        private int travelDurationSeconds;
        private String rideKey;
        private String routePatternKey;

        private static Event fromRow(Row row, ZoneId cityZone) {
            try {
                Event event = new Event();
                event.tripId = row.getAs("tripId");
                event.weekdayIso = ((Number) row.getAs("weekdayIso")).intValue();
                event.plate = row.getAs("plate");
                event.internalRouteId = row.getAs("internalRouteId");
                event.routeNumber = row.getAs("routeNumber");
                event.direction = ((Number) row.getAs("direction")).intValue();
                event.segmentId = row.getAs("segmentId");
                event.startStopId = row.getAs("startStopId");
                event.startStopName = row.getAs("startStopName");
                event.endStopId = row.getAs("endStopId");
                event.endStopName = row.getAs("endStopName");
                event.startStopOrder = ((Number) row.getAs("startStopOrder")).intValue();
                event.endStopOrder = ((Number) row.getAs("endStopOrder")).intValue();
                event.departureInstant = rowInstant(row.getAs("startExitedStopAt"));
                event.arrivalInstant = rowInstant(row.getAs("endEnteredStopAt"));
                event.departureSecond = secondOfDay(event.departureInstant, cityZone);
                event.arrivalSecond = secondOfDay(event.arrivalInstant, cityZone);
                event.travelDurationSeconds = ((Number) row.getAs("travelDurationSeconds")).intValue();
                if (event.arrivalSecond <= event.departureSecond || event.travelDurationSeconds <= 0) {
                    return null;
                }
                event.rideKey = event.internalRouteId + "|" + event.routeNumber + "|" + event.direction + "|" + event.plate;
                event.routePatternKey = event.internalRouteId + "|" + event.routeNumber + "|" + event.direction;
                return event;
            } catch (Exception e) {
                return null;
            }
        }

        private static Event syntheticSkippedStop(Event last, StopOnRoute target, int arrivalSecond) {
            Event event = new Event();
            event.tripId = "synthetic-skip|" + last.tripId + "|" + target.stopId;
            event.weekdayIso = last.weekdayIso;
            event.plate = last.plate;
            event.internalRouteId = last.internalRouteId;
            event.routeNumber = last.routeNumber;
            event.direction = last.direction;
            event.segmentId = last.segmentId + "|skip|" + target.stopId;
            event.startStopOrder = last.endStopOrder;
            event.endStopOrder = target.stopOrder;
            event.startStopId = last.endStopId;
            event.startStopName = last.endStopName;
            event.endStopId = target.stopId;
            event.endStopName = target.stopName;
            event.departureSecond = last.arrivalSecond;
            event.arrivalSecond = arrivalSecond;
            event.departureInstant = last.arrivalInstant;
            event.arrivalInstant = last.arrivalInstant.plusSeconds(Math.max(0, arrivalSecond - last.arrivalSecond));
            event.travelDurationSeconds = Math.max(0, arrivalSecond - last.arrivalSecond);
            event.rideKey = last.rideKey;
            event.routePatternKey = last.routePatternKey;
            return event;
        }

        private String rideKey() {
            return rideKey;
        }

        private String routePatternKey() {
            return routePatternKey;
        }
    }

    private static final class SearchIndex {
        private final Map<String, List<Event>> eventsByRideKey;
        private final IdentityHashMap<Event, Integer> eventIndexByIdentity;
        private final Map<String, Set<String>> routePatternsByStop;
        private final Map<String, StopOnRoute> stopsByRoutePatternStop;
        private final Set<String> transferStops;

        private SearchIndex(
                Map<String, List<Event>> eventsByRideKey,
                IdentityHashMap<Event, Integer> eventIndexByIdentity,
                Map<String, Set<String>> routePatternsByStop,
                Map<String, StopOnRoute> stopsByRoutePatternStop,
                Set<String> transferStops
        ) {
            this.eventsByRideKey = eventsByRideKey;
            this.eventIndexByIdentity = eventIndexByIdentity;
            this.routePatternsByStop = routePatternsByStop;
            this.stopsByRoutePatternStop = stopsByRoutePatternStop;
            this.transferStops = transferStops;
        }

        private static SearchIndex from(List<Event> events, Map<String, List<Event>> eventsByStartStop) {
            Map<String, List<Event>> eventsByRideKey = events.stream()
                    .collect(Collectors.groupingBy(Event::rideKey));
            IdentityHashMap<Event, Integer> eventIndexByIdentity = new IdentityHashMap<>();
            for (List<Event> rideEvents : eventsByRideKey.values()) {
                rideEvents.sort(Comparator
                        .comparingInt((Event event) -> event.departureSecond)
                        .thenComparing(event -> event.arrivalSecond)
                        .thenComparing(event -> event.startStopId)
                        .thenComparing(event -> event.endStopId));
                for (int i = 0; i < rideEvents.size(); i++) {
                    eventIndexByIdentity.put(rideEvents.get(i), i);
                }
            }

            Map<String, Set<String>> routePatternsByStop = new HashMap<>();
            Map<String, StopOnRoute> stopsByRoutePatternStop = new HashMap<>();
            for (Map.Entry<String, List<Event>> entry : eventsByStartStop.entrySet()) {
                Set<String> patterns = routePatternsByStop.computeIfAbsent(entry.getKey(), ignored -> new HashSet<>());
                for (Event event : entry.getValue()) {
                    patterns.add(event.routePatternKey());
                    stopsByRoutePatternStop.putIfAbsent(
                            event.routePatternKey() + "|" + event.startStopId,
                            new StopOnRoute(event.startStopId, event.startStopName, event.startStopOrder)
                    );
                    stopsByRoutePatternStop.putIfAbsent(
                            event.routePatternKey() + "|" + event.endStopId,
                            new StopOnRoute(event.endStopId, event.endStopName, event.endStopOrder)
                    );
                }
            }
            for (Event event : events) {
                routePatternsByStop.computeIfAbsent(event.endStopId, ignored -> new HashSet<>()).add(event.routePatternKey());
                stopsByRoutePatternStop.putIfAbsent(
                        event.routePatternKey() + "|" + event.startStopId,
                        new StopOnRoute(event.startStopId, event.startStopName, event.startStopOrder)
                );
                stopsByRoutePatternStop.putIfAbsent(
                        event.routePatternKey() + "|" + event.endStopId,
                        new StopOnRoute(event.endStopId, event.endStopName, event.endStopOrder)
                );
            }
            Set<String> transferStops = routePatternsByStop.entrySet().stream()
                    .filter(entry -> entry.getValue().size() > 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());
            return new SearchIndex(eventsByRideKey, eventIndexByIdentity, routePatternsByStop, stopsByRoutePatternStop, transferStops);
        }

        private boolean isTransferStop(String stopId) {
            return transferStops.contains(stopId);
        }

        private StopOnRoute stopOnRoute(String routePatternKey, String stopId) {
            return stopsByRoutePatternStop.get(routePatternKey + "|" + stopId);
        }
    }

    private static final class StopOnRoute {
        private final String stopId;
        private final String stopName;
        private final int stopOrder;

        private StopOnRoute(String stopId, String stopName, int stopOrder) {
            this.stopId = stopId;
            this.stopName = stopName;
            this.stopOrder = stopOrder;
        }
    }

    private enum SearchMode {
        LEGACY,
        ROUTE_NETWORK,
        STATIC_GRAPH_CACHE;

        private static SearchMode parse(String text) {
            if ("legacy".equalsIgnoreCase(text)) {
                return LEGACY;
            }
            if ("static-graph-cache".equalsIgnoreCase(text) || "static_graph_cache".equalsIgnoreCase(text)) {
                return STATIC_GRAPH_CACHE;
            }
            return ROUTE_NETWORK;
        }
    }

    private static final class StaticGraphCache {
        private final Map<String, List<StaticPathCandidate>> candidatesByOrigin;
        private final Path perOriginDir;

        private StaticGraphCache(Map<String, List<StaticPathCandidate>> candidatesByOrigin) {
            this.candidatesByOrigin = candidatesByOrigin;
            this.perOriginDir = null;
        }

        private StaticGraphCache(Path perOriginDir) {
            int maxCachedOrigins = Math.max(0, Integer.parseInt(System.getenv().getOrDefault(
                    "BUS_TRANSFER_STATIC_ORIGIN_CACHE_SIZE",
                    "16"
            )));
            this.candidatesByOrigin = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, List<StaticPathCandidate>> eldest) {
                    return size() > maxCachedOrigins;
                }
            };
            this.perOriginDir = perOriginDir;
        }

        private static StaticGraphCache empty() {
            return new StaticGraphCache(Map.of());
        }

        private static StaticGraphCache load(Path cacheDir, int maxTransfers) throws Exception {
            Path perOriginDir = cacheDir.resolve("paths-max-transfers-" + maxTransfers + "-by-origin");
            if (Files.isDirectory(perOriginDir)) {
                System.out.printf(Locale.ROOT, "BusTransferPotentialJob: using per-origin binary static graph cache %s%n", perOriginDir);
                return new StaticGraphCache(perOriginDir);
            }
            Path binaryFile = cacheDir.resolve("paths-max-transfers-" + maxTransfers + ".bin");
            if (Files.exists(binaryFile)) {
                return loadBinary(binaryFile);
            }
            Path file = cacheDir.resolve("paths-max-transfers-" + maxTransfers + ".jsonl");
            if (!Files.exists(file)) {
                throw new IllegalStateException("Static transfer graph cache not found: " + binaryFile + " or " + file);
            }
            Map<String, List<StaticPathCandidate>> result = new HashMap<>();
            long rows = 0L;
            long accepted = 0L;
            try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
                var iterator = lines.iterator();
                while (iterator.hasNext()) {
                    rows++;
                    StaticPathCandidate candidate = StaticPathCandidate.fromJson(MAPPER.readTree(iterator.next()));
                    if (candidate == null || candidate.edges.isEmpty()) {
                        continue;
                    }
                    result.computeIfAbsent(candidate.originStopId, ignored -> new ArrayList<>()).add(candidate);
                    accepted++;
                }
            }
            for (List<StaticPathCandidate> candidates : result.values()) {
                candidates.sort(Comparator
                        .comparingInt((StaticPathCandidate candidate) -> candidate.rideCount)
                        .thenComparingDouble(candidate -> candidate.totalDistanceMeters)
                        .thenComparing(candidate -> candidate.destinationStopId));
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferPotentialJob: loaded static graph cache %s rows=%d accepted=%d origins=%d%n",
                    file,
                    rows,
                    accepted,
                    result.size()
            );
            return new StaticGraphCache(result);
        }

        private List<StaticPathCandidate> candidatesForOrigin(String originStopId) {
            if (perOriginDir == null) {
                return candidatesByOrigin.get(originStopId);
            }
            List<StaticPathCandidate> cached = candidatesByOrigin.get(originStopId);
            if (cached != null) {
                return cached;
            }
            Path file = perOriginDir.resolve(originStopId + ".bin");
            if (!Files.exists(file)) {
                candidatesByOrigin.put(originStopId, List.of());
                return List.of();
            }
            try {
                Map<String, List<StaticPathCandidate>> loaded = readBinaryFile(file);
                List<StaticPathCandidate> candidates = loaded.getOrDefault(originStopId, List.of());
                candidates.sort(Comparator
                        .comparingInt((StaticPathCandidate candidate) -> candidate.rideCount)
                        .thenComparingDouble(candidate -> candidate.totalDistanceMeters)
                        .thenComparing(candidate -> candidate.destinationStopId));
                candidatesByOrigin.put(originStopId, candidates);
                return candidates;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load static graph origin cache " + file, e);
            }
        }

        private static StaticGraphCache loadBinary(Path file) throws Exception {
            Map<String, List<StaticPathCandidate>> result = readBinaryFile(file);
            long accepted = result.values().stream().mapToLong(List::size).sum();
            long rows = accepted;
            for (List<StaticPathCandidate> candidates : result.values()) {
                candidates.sort(Comparator
                        .comparingInt((StaticPathCandidate candidate) -> candidate.rideCount)
                        .thenComparingDouble(candidate -> candidate.totalDistanceMeters)
                        .thenComparing(candidate -> candidate.destinationStopId));
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferPotentialJob: loaded binary static graph cache %s rows=%d accepted=%d origins=%d%n",
                    file,
                    rows,
                    accepted,
                    result.size()
            );
            return new StaticGraphCache(result);
        }

        private static Map<String, List<StaticPathCandidate>> readBinaryFile(Path file) throws Exception {
            Map<String, List<StaticPathCandidate>> result = new HashMap<>();
            try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
                int magic = input.readInt();
                int version = input.readInt();
                if (magic != STATIC_GRAPH_BINARY_MAGIC || version != STATIC_GRAPH_BINARY_VERSION) {
                    throw new IllegalStateException("Unsupported static graph binary cache: " + file);
                }
                List<String> stopIds = readStringDictionary(input);
                List<String> routeIds = readStringDictionary(input);
                List<String> routeNumbers = readStringDictionary(input);
                int originCount = input.readInt();
                for (int originIndex = 0; originIndex < originCount; originIndex++) {
                    String originStopId = stopIds.get(input.readInt());
                    int candidateCount = input.readInt();
                    List<StaticPathCandidate> candidates = result.computeIfAbsent(originStopId, ignored -> new ArrayList<>());
                    for (int i = 0; i < candidateCount; i++) {
                        String destinationStopId = stopIds.get(input.readInt());
                        int rideCount = input.readInt();
                        double totalDistanceMeters = input.readInt();
                        input.readInt(); // alternativeIndex; ordering is already encoded by file order.
                        int edgeCount = input.readInt();
                        List<StaticEdge> edges = new ArrayList<>(edgeCount);
                        for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
                            int type = input.readUnsignedByte();
                            String fromStopId = stopIds.get(input.readInt());
                            String toStopId = stopIds.get(input.readInt());
                            double distanceMeters = input.readInt();
                            if (type == 0) {
                                edges.add(new StaticEdge(true, fromStopId, toStopId, distanceMeters));
                                continue;
                            }
                            String internalRouteId = routeIds.get(input.readInt());
                            String routeNumber = routeNumbers.get(input.readInt());
                            int direction = input.readInt();
                            edges.add(new StaticRideEdge(
                                    fromStopId,
                                    toStopId,
                                    distanceMeters,
                                    internalRouteId,
                                    routeNumber,
                                    direction
                            ));
                        }
                        if (!edges.isEmpty()) {
                            candidates.add(new StaticPathCandidate(
                                    originStopId,
                                    destinationStopId,
                                    rideCount,
                                    totalDistanceMeters,
                                    edges
                            ));
                        }
                    }
                }
            }
            return result;
        }

        private static List<String> readStringDictionary(DataInputStream input) throws Exception {
            int count = input.readInt();
            List<String> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                values.add(input.readUTF());
            }
            return values;
        }
    }

    private static final class StaticPathCandidate {
        private final String originStopId;
        private final String destinationStopId;
        private final int rideCount;
        private final double totalDistanceMeters;
        private final List<StaticEdge> edges;

        private StaticPathCandidate(
                String originStopId,
                String destinationStopId,
                int rideCount,
                double totalDistanceMeters,
                List<StaticEdge> edges
        ) {
            this.originStopId = originStopId;
            this.destinationStopId = destinationStopId;
            this.rideCount = rideCount;
            this.totalDistanceMeters = totalDistanceMeters;
            this.edges = edges;
        }

        private static StaticPathCandidate fromJson(JsonNode node) {
            if (node == null || !node.hasNonNull("originStopId") || !node.hasNonNull("destinationStopId")) {
                return null;
            }
            List<StaticEdge> edges = parseEdges(node);
            if (edges.isEmpty()) {
                return null;
            }
            return new StaticPathCandidate(
                    node.path("originStopId").asText(),
                    node.path("destinationStopId").asText(),
                    node.path("rideCount").asInt((int) edges.stream().filter(edge -> !edge.walk).count()),
                    node.path("totalDistanceMeters").asDouble(0.0),
                    edges
            );
        }

        private static List<StaticEdge> parseEdges(JsonNode node) {
            JsonNode edgesNode = node.path("edges");
            if (!edgesNode.isArray() || edgesNode.isEmpty()) {
                edgesNode = node.path("legs");
            }
            List<StaticEdge> edges = new ArrayList<>();
            if (!edgesNode.isArray()) {
                return edges;
            }
            for (JsonNode edge : edgesNode) {
                String type = edge.path("type").asText("ride");
                if ("walk".equalsIgnoreCase(type)) {
                    String fromStopId = edge.path("fromStopId").asText("");
                    String toStopId = edge.path("toStopId").asText("");
                    if (!fromStopId.isBlank() && !toStopId.isBlank()) {
                        edges.add(new StaticEdge(true, fromStopId, toStopId, edge.path("distanceMeters").asDouble(0.0)));
                    }
                    continue;
                }
                String fromStopId = edge.path("fromStopId").asText("");
                String toStopId = edge.path("toStopId").asText("");
                String internalRouteId = edge.path("internalRouteId").asText("");
                if (!fromStopId.isBlank() && !toStopId.isBlank() && !internalRouteId.isBlank()) {
                    edges.add(new StaticRideEdge(
                            fromStopId,
                            toStopId,
                            edge.path("distanceMeters").asDouble(0.0),
                            internalRouteId,
                            edge.path("routeNumber").asText(""),
                            edge.path("direction").asInt(Integer.MIN_VALUE)
                    ));
                }
            }
            return edges;
        }
    }

    private static class StaticEdge {
        protected final boolean walk;
        protected final String fromStopId;
        protected final String toStopId;
        protected final double distanceMeters;

        private StaticEdge(boolean walk, String fromStopId, String toStopId, double distanceMeters) {
            this.walk = walk;
            this.fromStopId = fromStopId;
            this.toStopId = toStopId;
            this.distanceMeters = distanceMeters;
        }
    }

    private static final class StaticRideEdge extends StaticEdge {
        private final String internalRouteId;
        private final String routeNumber;
        private final int direction;

        private StaticRideEdge(
                String fromStopId,
                String toStopId,
                double distanceMeters,
                String internalRouteId,
                String routeNumber,
                int direction
        ) {
            super(false, fromStopId, toStopId, distanceMeters);
            this.internalRouteId = internalRouteId;
            this.routeNumber = routeNumber;
            this.direction = direction;
        }

        private boolean matches(Event event) {
            return fromStopId.equals(event.startStopId)
                    && internalRouteId.equals(event.internalRouteId)
                    && (routeNumber.isBlank() || routeNumber.equals(event.routeNumber))
                    && (direction == Integer.MIN_VALUE || direction == event.direction);
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
