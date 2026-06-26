import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

public class BusAccessibilityMapCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));
    private static final double WALK_SPEED_MPS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_WALK_SPEED_MPS", "1.3"));
    private static final double WALK_RADIUS_METERS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_WALK_RADIUS_METERS", "2000"));
    private static final double STOP_CONNECT_RADIUS_METERS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_STOP_CONNECT_RADIUS_METERS", "180"));
    private static final double STOP_CLUSTER_RADIUS_METERS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_STOP_CLUSTER_RADIUS_METERS", "180"));
    private static final int STOP_CONNECT_FALLBACK_NODES = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_STOP_CONNECT_FALLBACK_NODES", "3"));
    private static final double SAMPLE_METERS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_SAMPLE_METERS", "100"));
    private static final int MAX_RIDES = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_MAX_RIDES", "10"));
    private static final int MAX_CANDIDATE_EVENTS_PER_STOP = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_MAX_CANDIDATE_EVENTS_PER_STOP", "120"));
    private static final int MIN_ZOOM = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_TILE_MIN_ZOOM", "10"));
    private static final int MAX_ZOOM = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_TILE_MAX_ZOOM", "17"));
    private static final double COLOR_MIN_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MIN_MINUTES", "0"));
    private static final double COLOR_MAX_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MAX_MINUTES", "0"));
    private static final double COLOR_MAX_PERCENTILE = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MAX_PERCENTILE", "0.90"));
    private static final double[] CONTOUR_THRESHOLDS_MINUTES = new double[]{15.0, 30.0, 60.0};
    private static final double ROAD_NODE_GRID_DEGREES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_ROAD_NODE_GRID_DEGREES", "0.005"));
    private static final int TILE_SIZE = 256;

    public static void main(String[] args) throws Exception {
        Path trafficBehaviorDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "/home/eljah/data/buscrawl/traffic-behavior"
        ));
        Path osmRoadsFile = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_OSM_ROADS_FILE",
                "/home/eljah/apps/buscrawl/osm-cache/overpass-kazan-roads.json"
        ));
        Path outputFile = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_MAP_CACHE_FILE",
                "/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map.json"
        ));
        Path transferPotentialDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_POTENTIAL_DIR",
                "/home/eljah/data/buscrawl/transfer-potential"
        ));
        Path tileRoot = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_TILE_ROOT",
                outputFile.getParent().resolve("tiles/accessibility/current").toString()
        ));
        Path tileBaseRoot = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_TILE_BASE_ROOT",
                tileRoot.getParent() == null ? tileRoot.toString() : tileRoot.getParent().toString()
        ));
        Path contourStatsDir = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_CONTOUR_STATS_DIR",
                outputFile.getParent().resolve("accessibility-contour-stats").toString()
        ));
        String originStopQuery = System.getenv().getOrDefault("BUS_ACCESSIBILITY_ORIGIN_STOP", "ПО Тасма");
        String sourceMode = System.getenv().getOrDefault("BUS_ACCESSIBILITY_SOURCE", "transfer-potential").trim();
        LocalDate serviceDate = LocalDate.parse(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_SERVICE_DATE",
                LocalDate.now(CITY_ZONE).minusDays(1).toString()
        ));
        LocalTime departureTime = LocalTime.parse(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEPARTURE_TIME", "08:00"));
        List<LocalDate> serviceDates = parseServiceDates(serviceDate);
        List<LocalTime> departureTimes = parseDepartureTimes(departureTime);
        Set<String> renderModes = parseRenderModes();
        String sparkMaster = System.getenv().getOrDefault("BUS_ACCESSIBILITY_SPARK_MASTER", "local[2]");
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_SPARK_LOCAL_DIR",
                "/home/eljah/data/buscrawl/accessibility-map-spark-temp"
        ));

        Files.createDirectories(outputFile.getParent());
        Files.createDirectories(sparkLocalDir);

        SparkSession spark = SparkSession.builder()
                .appName("BusAccessibilityMapCacheJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_ACCESSIBILITY_SPARK_DRIVER_MEMORY", "4g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_ACCESSIBILITY_SPARK_EXECUTOR_MEMORY", "4g"))
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            Map<String, StopPoint> stopsById = loadStopsById();
            List<RoadWay> roads = loadRoadWays(osmRoadsFile);
            RoadGraph roadGraph = RoadGraph.build(roads);
            boolean useTransferPotential = "transfer-potential".equalsIgnoreCase(sourceMode);
            List<StopPoint> transferOriginStops = useTransferPotential
                    ? selectOriginStops(stopsById, originStopQuery)
                    : List.of();
            if (useTransferPotential && transferOriginStops.isEmpty()) {
                throw new IllegalStateException("Origin stop not found in routes.json, query='" + originStopQuery + "'");
            }
            List<SnapshotPayload> snapshots = new ArrayList<>();
            for (LocalDate snapshotDate : serviceDates) {
                Map<String, List<Event>> segmentEventsByStartStop = useTransferPotential
                        ? Map.of()
                        : loadSegmentEvents(spark, trafficBehaviorDir.resolve("segment-trips"), stopsById, snapshotDate);
                Map<Integer, List<StopTime>> transferStopTimesByBucket = useTransferPotential
                        ? loadTransferStopTimesByBucket(
                                spark,
                                transferPotentialDir.resolve("journeys"),
                                stopsById,
                                transferOriginStops,
                                snapshotDate
                        )
                        : Map.of();
                for (LocalTime snapshotTime : departureTimes) {
                    Reachability reachability;
                    try {
                        reachability = useTransferPotential
                                ? calculateReachabilityFromTransferCache(
                                        transferOriginStops,
                                        transferStopTimesByBucket,
                                        snapshotDate,
                                        snapshotTime
                                )
                                : calculateReachabilityFromEvents(
                                        stopsById,
                                        originStopQuery,
                                        snapshotDate,
                                        snapshotTime,
                                        segmentEventsByStartStop
                                );
                    } catch (IllegalStateException e) {
                        System.out.printf(
                                Locale.ROOT,
                                "BusAccessibilityMapCacheJob: skipped snapshot %s: %s%n",
                                snapshotId(snapshotDate, snapshotTime),
                                e.getMessage()
                        );
                        continue;
                    }
                    List<AccessibleSegment> segments = buildAccessibilitySegments(roadGraph, roads, reachability.stopTimes);
                    String snapshotId = snapshotId(snapshotDate, snapshotTime);
                    Path snapshotRoot = tileBaseRoot.resolve(snapshotId);
                    Path totalTileRoot = snapshotRoot.resolve("total");
                    Path totalFullTileRoot = snapshotRoot.resolve("total-full");
                    Path totalLogTileRoot = snapshotRoot.resolve("total-log");
                    Path walkTileRoot = snapshotRoot.resolve("walk");
                    Path stopTransportTileRoot = snapshotRoot.resolve("stop-transport");
                    Path contourTileRoot = snapshotRoot.resolve("contours");
                    Map<Integer, Map<String, List<AccessibleSegment>>> tileIndexes = buildTileIndexes(segments, Double.POSITIVE_INFINITY);
                    Map<Integer, Map<String, List<AccessibleSegment>>> contourTileIndexes = buildTileIndexes(
                            segments,
                            CONTOUR_THRESHOLDS_MINUTES[CONTOUR_THRESHOLDS_MINUTES.length - 1]
                    );
                    ColorScale totalColorScale = renderTiles(segments, totalTileRoot, ColorMetric.TOTAL, tileIndexes);
                    ColorScale totalFullColorScale = renderModes.contains("totalFull") ? renderTiles(segments, totalFullTileRoot, ColorMetric.TOTAL_FULL, tileIndexes) : null;
                    ColorScale totalLogColorScale = renderModes.contains("totalLog") ? renderTiles(segments, totalLogTileRoot, ColorMetric.TOTAL_LOG, tileIndexes) : null;
                    ColorScale walkColorScale = renderModes.contains("walk") ? renderTiles(segments, walkTileRoot, ColorMetric.WALK, tileIndexes) : null;
                    ColorScale stopTransportColorScale = renderModes.contains("stopTransport") ? renderTiles(segments, stopTransportTileRoot, ColorMetric.STOP_TRANSPORT, tileIndexes) : null;
                    ContourStats contourStats = calculateContourStats(segments);
                    renderContourTiles(contourTileRoot, contourTileIndexes);
                    snapshots.add(new SnapshotPayload(
                            snapshotId,
                            snapshotDate,
                            snapshotTime,
                            reachability,
                            segments.size(),
                            contourStats,
                            totalColorScale,
                            totalFullColorScale,
                            totalLogColorScale,
                            walkColorScale,
                            stopTransportColorScale
                    ));
                    writeContourStats(spark, contourStatsDir, snapshots);
                    writePayload(outputFile, tileBaseRoot, originStopQuery, sourceMode, roads, serviceDates, departureTimes, snapshots);
                    System.out.printf(
                            Locale.ROOT,
                            "BusAccessibilityMapCacheJob: rendered snapshot %s stops=%d segments=%d%n",
                            snapshotId,
                            reachability.stopTimes.size(),
                            segments.size()
                    );
                }
            }
            writePayload(outputFile, tileBaseRoot, originStopQuery, sourceMode, roads, serviceDates, departureTimes, snapshots);
            SnapshotPayload first = snapshots.isEmpty() ? null : snapshots.get(0);
            System.out.printf(
                    Locale.ROOT,
                    "BusAccessibilityMapCacheJob: wrote %d accessibility snapshots from %d roads using %s to %s and %s%n",
                    snapshots.size(),
                    roads.size(),
                    sourceMode,
                    outputFile,
                    tileBaseRoot
            );
        } finally {
            spark.stop();
        }
    }

    private static Reachability calculateReachability(
            SparkSession spark,
            Path segmentTripsDir,
            Map<String, StopPoint> stopsById,
            String originStopQuery,
            LocalDate serviceDate,
            LocalTime departureTime
    ) {
        return calculateReachabilityFromEvents(
                stopsById,
                originStopQuery,
                serviceDate,
                departureTime,
                loadSegmentEvents(spark, segmentTripsDir, stopsById, serviceDate)
        );
    }

    private static Map<String, List<Event>> loadSegmentEvents(
            SparkSession spark,
            Path segmentTripsDir,
            Map<String, StopPoint> stopsById,
            LocalDate serviceDate
    ) {
        Dataset<Row> rows = spark.read().parquet(segmentTripsDir.toAbsolutePath().toString())
                .filter(col("serviceDate").cast("string").equalTo(serviceDate.toString()))
                .select(
                        "tripId",
                        "plate",
                        "internalRouteId",
                        "routeNumber",
                        "segmentId",
                        "startStopId",
                        "startStopName",
                        "endStopId",
                        "endStopName",
                        "startExitedStopAt",
                        "endEnteredStopAt",
                        "travelDurationSeconds"
                );

        Map<String, List<Event>> eventsByStartStop = new HashMap<>();
        for (Row row : rows.collectAsList()) {
            Event event = Event.fromRow(row, serviceDate);
            if (event == null || !stopsById.containsKey(event.startStopId) || !stopsById.containsKey(event.endStopId)) {
                continue;
            }
            eventsByStartStop.computeIfAbsent(event.startStopId, ignored -> new ArrayList<>()).add(event);
        }
        for (List<Event> events : eventsByStartStop.values()) {
            events.sort(Comparator.comparingInt(event -> event.departureSecond));
        }
        return eventsByStartStop;
    }

    private static Reachability calculateReachabilityFromEvents(
            Map<String, StopPoint> stopsById,
            String originStopQuery,
            LocalDate serviceDate,
            LocalTime departureTime,
            Map<String, List<Event>> eventsByStartStop
    ) {
        List<StopPoint> originStops = selectOriginStops(stopsById, originStopQuery);
        if (originStops.isEmpty()) {
            throw new IllegalStateException("Origin stop not found in routes.json, query='" + originStopQuery + "'");
        }

        int requestedSecond = departureTime.toSecondOfDay();
        Map<String, StateNode> bestByDestination = findBestJourneys(
                originStops.stream().map(stop -> stop.stopId).collect(Collectors.toList()),
                requestedSecond,
                eventsByStartStop
        );
        for (StopPoint origin : originStops) {
            bestByDestination.put(origin.stopId, new StateNode(origin.stopId, requestedSecond, null, 0));
        }

        List<StopTime> stopTimes = bestByDestination.values().stream()
                .filter(node -> node.arrivalSecond >= requestedSecond)
                .map(node -> {
                    StopPoint stop = requireStop(stopsById, node.stopId);
                    return new StopTime(
                            stop.stopId,
                            stop.stopName,
                            stop.lat,
                            stop.lon,
                            node.arrivalSecond - requestedSecond
                    );
                })
                .sorted(Comparator.comparingInt(stop -> stop.transportSeconds))
                .collect(Collectors.toList());
        stopTimes = expandStopTimesToSameNameClusters(stopTimes, stopsById);
        return new Reachability(originStops, stopTimes);
    }

    private static List<LocalDate> parseServiceDates(LocalDate defaultDate) {
        String text = System.getenv().getOrDefault("BUS_ACCESSIBILITY_SERVICE_DATES", "").trim();
        if (text.isBlank()) {
            return List.of(defaultDate);
        }
        List<LocalDate> dates = new ArrayList<>();
        for (String part : text.split(",")) {
            String value = part.trim();
            if (!value.isBlank()) {
                dates.add(LocalDate.parse(value));
            }
        }
        dates.sort(Comparator.naturalOrder());
        return dates.isEmpty() ? List.of(defaultDate) : dates;
    }

    private static List<LocalTime> parseDepartureTimes(LocalTime defaultTime) {
        String explicit = System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEPARTURE_TIMES", "").trim();
        if (!explicit.isBlank()) {
            List<LocalTime> times = new ArrayList<>();
            for (String part : explicit.split(",")) {
                String value = part.trim();
                if (!value.isBlank()) {
                    times.add(LocalTime.parse(value));
                }
            }
            times.sort(Comparator.naturalOrder());
            return times.isEmpty() ? List.of(defaultTime) : times;
        }
        LocalTime start = LocalTime.parse(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEPARTURE_START", "04:00"));
        LocalTime end = LocalTime.parse(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEPARTURE_END", "23:45"));
        int stepMinutes = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES", "15"));
        if (stepMinutes <= 0) {
            throw new IllegalArgumentException("BUS_ACCESSIBILITY_DEPARTURE_STEP_MINUTES must be positive");
        }
        List<LocalTime> times = new ArrayList<>();
        int startSecond = start.toSecondOfDay();
        int endSecond = end.toSecondOfDay();
        if (endSecond < startSecond) {
            throw new IllegalArgumentException("BUS_ACCESSIBILITY_DEPARTURE_END must not be earlier than BUS_ACCESSIBILITY_DEPARTURE_START");
        }
        for (int second = startSecond; second <= endSecond; second += stepMinutes * 60) {
            times.add(LocalTime.ofSecondOfDay(second));
        }
        return times.isEmpty() ? List.of(defaultTime) : times;
    }

    private static Set<String> parseRenderModes() {
        String text = System.getenv().getOrDefault("BUS_ACCESSIBILITY_RENDER_MODES", "total,totalFull,totalLog,walk,stopTransport").trim();
        Set<String> modes = new HashSet<>();
        for (String part : text.split(",")) {
            String mode = part.trim();
            if (!mode.isBlank()) {
                modes.add(mode);
            }
        }
        modes.add("total");
        return modes;
    }

    private static String snapshotId(LocalDate serviceDate, LocalTime departureTime) {
        return serviceDate + "-" + String.format(Locale.ROOT, "%02d%02d", departureTime.getHour(), departureTime.getMinute());
    }

    private static Map<String, StateNode> findBestJourneys(
            List<String> originStopIds,
            int requestedSecond,
            Map<String, List<Event>> eventsByStartStop
    ) {
        PriorityQueue<StateNode> queue = new PriorityQueue<>(Comparator.comparingInt(node -> node.arrivalSecond));
        Map<String, Integer> bestStateTime = new HashMap<>();
        Map<String, StateNode> bestByDestination = new HashMap<>();
        for (String originStopId : originStopIds) {
            StateNode start = new StateNode(originStopId, requestedSecond, null, 0);
            queue.add(start);
            bestStateTime.put(stateKey(start), requestedSecond);
        }

        while (!queue.isEmpty()) {
            StateNode current = queue.poll();
            Integer known = bestStateTime.get(stateKey(current));
            if (known != null && current.arrivalSecond > known) {
                continue;
            }
            bestByDestination.merge(current.stopId, current, (left, right) -> left.arrivalSecond <= right.arrivalSecond ? left : right);

            List<Event> events = eventsByStartStop.getOrDefault(current.stopId, Collections.emptyList());
            int startIndex = lowerBoundByDeparture(events, current.arrivalSecond);
            Set<String> usedOutgoingKeys = new HashSet<>();
            int accepted = 0;
            for (int i = startIndex; i < events.size() && accepted < MAX_CANDIDATE_EVENTS_PER_STOP; i++) {
                Event event = events.get(i);
                String outgoingKey = event.routeNumber + "|" + event.plate + "|" + event.segmentId + "|" + event.endStopId;
                if (!usedOutgoingKeys.add(outgoingKey)) {
                    continue;
                }
                String rideKey = event.routeNumber + "|" + event.plate;
                int rideCount = rideKey.equals(current.rideKey) ? current.rideCount : current.rideCount + 1;
                if (rideCount > MAX_RIDES) {
                    continue;
                }
                StateNode next = new StateNode(event.endStopId, event.arrivalSecond, rideKey, rideCount);
                String nextKey = stateKey(next);
                Integer previousBest = bestStateTime.get(nextKey);
                if (previousBest == null || next.arrivalSecond < previousBest) {
                    bestStateTime.put(nextKey, next.arrivalSecond);
                    queue.add(next);
                }
                accepted++;
            }
        }
        for (String originStopId : originStopIds) {
            bestByDestination.remove(originStopId);
        }
        return bestByDestination;
    }

    private static Reachability calculateReachabilityFromTransferPotential(
            SparkSession spark,
            Path journeysDir,
            Map<String, StopPoint> stopsById,
            String originStopQuery,
            LocalDate serviceDate,
            LocalTime departureTime
    ) {
        List<StopPoint> originStops = selectOriginStops(stopsById, originStopQuery);
        if (originStops.isEmpty()) {
            throw new IllegalStateException("Origin stop not found in routes.json, query='" + originStopQuery + "'");
        }
        if (!Files.exists(journeysDir)) {
            throw new IllegalStateException("Transfer potential journeys directory not found: " + journeysDir);
        }

        int transferBucketMinutes = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_BUCKET_MINUTES", "10"));
        int requestedMinute = departureTime.toSecondOfDay() / 60;
        int bucketMinute = (requestedMinute / transferBucketMinutes) * transferBucketMinutes;
        String[] originIds = originStops.stream().map(stop -> stop.stopId).toArray(String[]::new);
        Dataset<Row> bestJourneys = spark.read().parquet(journeysDir.toAbsolutePath().toString())
                .filter(col("serviceDate").cast("string").equalTo(serviceDate.toString()))
                .filter(col("departureBucketMinute").equalTo(bucketMinute))
                .filter(col("originStopId").isin((Object[]) originIds))
                .filter(col("reachable").equalTo(true))
                .groupBy("destinationStopId")
                .agg(min("totalJourneySeconds").alias("transportSeconds"));

        List<StopTime> stopTimes = new ArrayList<>();
        for (Row row : bestJourneys.collectAsList()) {
            String stopId = row.getAs("destinationStopId");
            if (stopId == null || !stopsById.containsKey(stopId) || row.isNullAt(1)) {
                continue;
            }
            StopPoint stop = requireStop(stopsById, stopId);
            stopTimes.add(new StopTime(
                    stop.stopId,
                    stop.stopName,
                    stop.lat,
                    stop.lon,
                    ((Number) row.getAs("transportSeconds")).intValue()
            ));
        }
        for (StopPoint origin : originStops) {
            stopTimes.add(new StopTime(origin.stopId, origin.stopName, origin.lat, origin.lon, 0));
        }
        stopTimes = expandStopTimesToSameNameClusters(stopTimes, stopsById).stream()
                .collect(Collectors.toMap(
                        stop -> stop.stopId,
                        stop -> stop,
                        (left, right) -> left.transportSeconds <= right.transportSeconds ? left : right,
                        LinkedHashMap::new
                ))
                .values()
                .stream()
                .sorted(Comparator.comparingInt(stop -> stop.transportSeconds))
                .collect(Collectors.toList());
        if (stopTimes.size() <= originStops.size()) {
            throw new IllegalStateException(
                    "No transfer-potential journeys found for origin query='" + originStopQuery
                            + "', serviceDate=" + serviceDate + ", bucketMinute=" + bucketMinute
            );
        }
        return new Reachability(originStops, stopTimes);
    }

    private static Map<Integer, List<StopTime>> loadTransferStopTimesByBucket(
            SparkSession spark,
            Path journeysDir,
            Map<String, StopPoint> stopsById,
            List<StopPoint> originStops,
            LocalDate serviceDate
    ) {
        if (!Files.exists(journeysDir)) {
            throw new IllegalStateException("Transfer potential journeys directory not found: " + journeysDir);
        }
        String[] originIds = originStops.stream().map(stop -> stop.stopId).toArray(String[]::new);
        Dataset<Row> bestJourneys = spark.read().parquet(journeysDir.toAbsolutePath().toString())
                .filter(col("serviceDate").cast("string").equalTo(serviceDate.toString()))
                .filter(col("originStopId").isin((Object[]) originIds))
                .filter(col("reachable").equalTo(true))
                .groupBy("departureBucketMinute", "destinationStopId")
                .agg(min("totalJourneySeconds").alias("transportSeconds"));

        Map<Integer, List<StopTime>> byBucket = new HashMap<>();
        for (Row row : bestJourneys.collectAsList()) {
            Integer bucketMinute = ((Number) row.getAs("departureBucketMinute")).intValue();
            String stopId = row.getAs("destinationStopId");
            if (stopId == null || !stopsById.containsKey(stopId) || row.isNullAt(2)) {
                continue;
            }
            StopPoint stop = requireStop(stopsById, stopId);
            byBucket.computeIfAbsent(bucketMinute, ignored -> new ArrayList<>()).add(new StopTime(
                    stop.stopId,
                    stop.stopName,
                    stop.lat,
                    stop.lon,
                    ((Number) row.getAs("transportSeconds")).intValue()
            ));
        }
        for (Map.Entry<Integer, List<StopTime>> entry : byBucket.entrySet()) {
            List<StopTime> stopTimes = entry.getValue();
            for (StopPoint origin : originStops) {
                stopTimes.add(new StopTime(origin.stopId, origin.stopName, origin.lat, origin.lon, 0));
            }
            List<StopTime> expanded = expandStopTimesToSameNameClusters(stopTimes, stopsById).stream()
                    .collect(Collectors.toMap(
                            stop -> stop.stopId,
                            stop -> stop,
                            (left, right) -> left.transportSeconds <= right.transportSeconds ? left : right,
                            LinkedHashMap::new
                    ))
                    .values()
                    .stream()
                    .sorted(Comparator.comparingInt(stop -> stop.transportSeconds))
                    .collect(Collectors.toList());
            entry.setValue(expanded);
        }
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: loaded transfer reachability cache for %s buckets=%d origins=%d%n",
                serviceDate,
                byBucket.size(),
                originStops.size()
        );
        return byBucket;
    }

    private static Reachability calculateReachabilityFromTransferCache(
            List<StopPoint> originStops,
            Map<Integer, List<StopTime>> stopTimesByBucket,
            LocalDate serviceDate,
            LocalTime departureTime
    ) {
        int transferBucketMinutes = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_BUCKET_MINUTES", "10"));
        int requestedMinute = departureTime.toSecondOfDay() / 60;
        int bucketMinute = (requestedMinute / transferBucketMinutes) * transferBucketMinutes;
        List<StopTime> stopTimes = stopTimesByBucket.get(bucketMinute);
        if (stopTimes == null || stopTimes.size() <= originStops.size()) {
            throw new IllegalStateException(
                    "No transfer-potential journeys found for serviceDate=" + serviceDate
                            + ", bucketMinute=" + bucketMinute
            );
        }
        return new Reachability(originStops, stopTimes);
    }

    private static List<StopTime> expandStopTimesToSameNameClusters(
            List<StopTime> stopTimes,
            Map<String, StopPoint> stopsById
    ) {
        if (stopTimes.isEmpty()) {
            return stopTimes;
        }
        Map<String, Integer> bestTransportByStopId = stopTimes.stream()
                .collect(Collectors.toMap(
                        stop -> stop.stopId,
                        stop -> stop.transportSeconds,
                        Math::min,
                        HashMap::new
                ));
        Map<String, List<StopPoint>> allStopsByName = stopsById.values().stream()
                .collect(Collectors.groupingBy(stop -> normalizeStopName(stop.stopName)));
        for (StopTime reachable : stopTimes) {
            String name = normalizeStopName(reachable.stopName);
            List<StopPoint> sameNameStops = allStopsByName.getOrDefault(name, List.of());
            for (StopPoint candidate : sameNameStops) {
                if (candidate.stopId.equals(reachable.stopId)) {
                    continue;
                }
                if (haversineMeters(reachable.lat, reachable.lon, candidate.lat, candidate.lon) <= STOP_CLUSTER_RADIUS_METERS) {
                    bestTransportByStopId.merge(candidate.stopId, reachable.transportSeconds, Math::min);
                }
            }
        }
        return bestTransportByStopId.entrySet().stream()
                .map(entry -> {
                    StopPoint stop = requireStop(stopsById, entry.getKey());
                    return new StopTime(
                            stop.stopId,
                            stop.stopName,
                            stop.lat,
                            stop.lon,
                            entry.getValue()
                    );
                })
                .sorted(Comparator.comparingInt(stop -> stop.transportSeconds))
                .collect(Collectors.toList());
    }

    private static List<StopPoint> selectOriginStops(Map<String, StopPoint> stopsById, String originStopQuery) {
        String explicitIds = System.getenv().getOrDefault("BUS_ACCESSIBILITY_ORIGIN_STOP_IDS", "").trim();
        if (!explicitIds.isBlank()) {
            return List.of(explicitIds.split(",")).stream()
                    .map(String::trim)
                    .filter(id -> !id.isBlank())
                    .map(stopsById::get)
                    .filter(stop -> stop != null)
                    .sorted(Comparator.comparing(stop -> stop.stopId))
                    .collect(Collectors.toList());
        }
        return stopsById.values().stream()
                .filter(stop -> stop.stopName.toLowerCase(Locale.ROOT).contains(originStopQuery.toLowerCase(Locale.ROOT)))
                .sorted(Comparator.comparing(stop -> stop.stopId))
                .collect(Collectors.toList());
    }

    private static int lowerBoundByDeparture(List<Event> events, int departureSecond) {
        int low = 0;
        int high = events.size();
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (events.get(mid).departureSecond < departureSecond) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private static String stateKey(StateNode node) {
        return node.stopId + "|" + node.rideCount + "|" + (node.rideKey == null ? "" : node.rideKey);
    }

    private static Map<String, StopPoint> loadStopsById() {
        RouteTopology topology = RouteTopology.load(System.getenv().getOrDefault("BUS_ROUTE_TOPOLOGY_FILE", ""));
        Map<String, StopPoint> stopsById = new LinkedHashMap<>();
        for (Map<String, Object> stop : topology.buildStops()) {
            String stopId = String.valueOf(stop.get("stopId"));
            stopsById.put(stopId, new StopPoint(
                    stopId,
                    String.valueOf(stop.get("stopName")),
                    ((Number) stop.get("latitude")).doubleValue(),
                    ((Number) stop.get("longitude")).doubleValue()
            ));
        }
        return stopsById;
    }

    private static StopPoint requireStop(Map<String, StopPoint> stopsById, String stopId) {
        StopPoint stop = stopsById.get(stopId);
        if (stop == null) {
            throw new IllegalStateException("Stop coordinates not found in routes.json for stopId=" + stopId);
        }
        return stop;
    }

    private static List<RoadWay> loadRoadWays(Path osmRoadsFile) throws Exception {
        JSONObject root = new JSONObject(Files.readString(osmRoadsFile));
        JSONArray elements = root.getJSONArray("elements");
        List<RoadWay> roads = new ArrayList<>();
        for (int i = 0; i < elements.length(); i++) {
            JSONObject element = elements.getJSONObject(i);
            if (!"way".equals(element.optString("type")) || !element.has("geometry") || !element.has("tags")) {
                continue;
            }
            JSONObject tags = element.getJSONObject("tags");
            String highway = tags.optString("highway", "");
            if (!isUsefulRoad(highway)) {
                continue;
            }
            JSONArray geometry = element.getJSONArray("geometry");
            if (geometry.length() < 2) {
                continue;
            }
            List<Point> points = new ArrayList<>();
            for (int j = 0; j < geometry.length(); j++) {
                JSONObject point = geometry.getJSONObject(j);
                points.add(new Point(point.getDouble("lat"), point.getDouble("lon")));
            }
            roads.add(new RoadWay(element.getLong("id"), highway, tags.optString("name", ""), points));
        }
        return roads;
    }

    private static boolean isUsefulRoad(String highway) {
        if (highway == null || highway.isBlank()) {
            return false;
        }
        return !List.of("motorway", "motorway_link", "construction", "proposed", "platform", "elevator").contains(highway);
    }

    private static List<AccessibleSegment> buildAccessibilitySegments(RoadGraph graph, List<RoadWay> roads, List<StopTime> stops) {
        Map<String, AccessLabel> accessByNode = calculateRoadGraphAccess(graph, stops);
        Map<String, AccessLabel> nearestStopByNode = calculateRoadGraphNearestStopAccess(graph, stops);
        List<AccessibleSegment> segments = new ArrayList<>();
        for (RoadWay road : roads) {
            for (int i = 1; i < road.points.size(); i++) {
                Point a = road.points.get(i - 1);
                Point b = road.points.get(i);
                int aNode = graph.nodeIdsByPointKey.getOrDefault(pointKey(a), -1);
                int bNode = graph.nodeIdsByPointKey.getOrDefault(pointKey(b), -1);
                AccessLabel aAccess = aNode >= 0 ? accessByNode.get(String.valueOf(aNode)) : null;
                AccessLabel bAccess = bNode >= 0 ? accessByNode.get(String.valueOf(bNode)) : null;
                AccessLabel aNearest = aNode >= 0 ? nearestStopByNode.get(String.valueOf(aNode)) : null;
                AccessLabel bNearest = bNode >= 0 ? nearestStopByNode.get(String.valueOf(bNode)) : null;
                if (aAccess == null && bAccess == null) {
                    continue;
                }
                double distance = haversineMeters(a.lat, a.lon, b.lat, b.lon);
                int pieces = Math.max(1, (int) Math.ceil(distance / SAMPLE_METERS));
                for (int piece = 0; piece < pieces; piece++) {
                    double t0 = piece / (double) pieces;
                    double t1 = (piece + 1) / (double) pieces;
                    Point p0 = interpolate(a, b, t0);
                    Point p1 = interpolate(a, b, t1);
                    Point mid = interpolate(a, b, (t0 + t1) / 2.0);
                    BestAccess best = bestAccessOnRoadSegment(a, b, mid, distance, aAccess, bAccess);
                    if (best == null) {
                        continue;
                    }
                    BestAccess nearest = nearestAccessOnRoadSegment(a, b, mid, distance, aNearest, bNearest);
                    if (nearest == null) {
                        nearest = best;
                    }
                    segments.add(new AccessibleSegment(
                            p0,
                            p1,
                            best.totalSeconds / 60.0,
                            best.walkSeconds / 60.0,
                            best.walkDistanceMeters,
                            best.stop.transportSeconds / 60.0,
                            nearest.stop.transportSeconds / 60.0,
                            nearest.walkDistanceMeters,
                            haversineMeters(p0.lat, p0.lon, p1.lat, p1.lon),
                            best.stop.stopId,
                            best.stop.stopName
                    ));
                }
            }
        }
        return segments;
    }

    private static Map<String, AccessLabel> calculateRoadGraphAccess(RoadGraph graph, List<StopTime> stops) {
        PriorityQueue<AccessState> queue = new PriorityQueue<>(Comparator.comparingDouble(state -> state.totalSeconds));
        Map<String, AccessLabel> bestByNode = new HashMap<>();
        for (StopTime stop : stops) {
            List<NodeDistance> connectors = graph.closestNodes(stop.lat, stop.lon, STOP_CONNECT_RADIUS_METERS, STOP_CONNECT_FALLBACK_NODES);
            for (NodeDistance connector : connectors) {
                double walkSeconds = connector.distanceMeters / WALK_SPEED_MPS;
                double totalSeconds = stop.transportSeconds + walkSeconds;
                AccessState state = new AccessState(
                        connector.nodeId,
                        totalSeconds,
                        connector.distanceMeters,
                        stop
                );
                String key = String.valueOf(connector.nodeId);
                AccessLabel previous = bestByNode.get(key);
                if (previous == null || state.totalSeconds < previous.totalSeconds) {
                    bestByNode.put(key, new AccessLabel(stop, state.totalSeconds, state.walkMeters));
                    queue.add(state);
                }
            }
        }

        while (!queue.isEmpty()) {
            AccessState current = queue.poll();
            String currentKey = String.valueOf(current.nodeId);
            AccessLabel known = bestByNode.get(currentKey);
            if (known == null || current.totalSeconds > known.totalSeconds + 0.001) {
                continue;
            }
            for (RoadEdge edge : graph.edgesByNode.getOrDefault(current.nodeId, List.of())) {
                double nextWalkMeters = current.walkMeters + edge.distanceMeters;
                if (nextWalkMeters > WALK_RADIUS_METERS) {
                    continue;
                }
                double nextTotalSeconds = current.totalSeconds + edge.distanceMeters / WALK_SPEED_MPS;
                String nextKey = String.valueOf(edge.toNodeId);
                AccessLabel previous = bestByNode.get(nextKey);
                if (previous == null || nextTotalSeconds < previous.totalSeconds) {
                    bestByNode.put(nextKey, new AccessLabel(current.stop, nextTotalSeconds, nextWalkMeters));
                    queue.add(new AccessState(edge.toNodeId, nextTotalSeconds, nextWalkMeters, current.stop));
                }
            }
        }
        return bestByNode;
    }

    private static Map<String, AccessLabel> calculateRoadGraphNearestStopAccess(RoadGraph graph, List<StopTime> stops) {
        List<StopTime> clusteredStops = stopsWithClusterBestTransport(stops);
        PriorityQueue<AccessState> queue = new PriorityQueue<>(Comparator.comparingDouble(state -> state.walkMeters));
        Map<String, AccessLabel> bestByNode = new HashMap<>();
        for (StopTime stop : clusteredStops) {
            List<NodeDistance> connectors = graph.closestNodes(stop.lat, stop.lon, STOP_CONNECT_RADIUS_METERS, STOP_CONNECT_FALLBACK_NODES);
            for (NodeDistance connector : connectors) {
                double walkSeconds = connector.distanceMeters / WALK_SPEED_MPS;
                double totalSeconds = stop.transportSeconds + walkSeconds;
                AccessState state = new AccessState(
                        connector.nodeId,
                        totalSeconds,
                        connector.distanceMeters,
                        stop
                );
                String key = String.valueOf(connector.nodeId);
                AccessLabel previous = bestByNode.get(key);
                if (previous == null || state.walkMeters < previous.walkMeters) {
                    bestByNode.put(key, new AccessLabel(stop, state.totalSeconds, state.walkMeters));
                    queue.add(state);
                }
            }
        }

        while (!queue.isEmpty()) {
            AccessState current = queue.poll();
            String currentKey = String.valueOf(current.nodeId);
            AccessLabel known = bestByNode.get(currentKey);
            if (known == null || current.walkMeters > known.walkMeters + 0.001) {
                continue;
            }
            for (RoadEdge edge : graph.edgesByNode.getOrDefault(current.nodeId, List.of())) {
                double nextWalkMeters = current.walkMeters + edge.distanceMeters;
                if (nextWalkMeters > WALK_RADIUS_METERS) {
                    continue;
                }
                double nextTotalSeconds = current.stop.transportSeconds + nextWalkMeters / WALK_SPEED_MPS;
                String nextKey = String.valueOf(edge.toNodeId);
                AccessLabel previous = bestByNode.get(nextKey);
                if (previous == null || nextWalkMeters < previous.walkMeters) {
                    bestByNode.put(nextKey, new AccessLabel(current.stop, nextTotalSeconds, nextWalkMeters));
                    queue.add(new AccessState(edge.toNodeId, nextTotalSeconds, nextWalkMeters, current.stop));
                }
            }
        }
        return bestByNode;
    }

    private static List<StopTime> stopsWithClusterBestTransport(List<StopTime> stops) {
        Map<String, List<StopTime>> byName = stops.stream()
                .collect(Collectors.groupingBy(stop -> normalizeStopName(stop.stopName)));
        List<StopTime> result = new ArrayList<>(stops.size());
        for (List<StopTime> sameNameStops : byName.values()) {
            if (sameNameStops.size() == 1) {
                result.add(sameNameStops.get(0));
                continue;
            }
            int[] parent = new int[sameNameStops.size()];
            for (int i = 0; i < parent.length; i++) {
                parent[i] = i;
            }
            for (int i = 0; i < sameNameStops.size(); i++) {
                StopTime left = sameNameStops.get(i);
                for (int j = i + 1; j < sameNameStops.size(); j++) {
                    StopTime right = sameNameStops.get(j);
                    double distance = haversineMeters(left.lat, left.lon, right.lat, right.lon);
                    if (distance <= STOP_CLUSTER_RADIUS_METERS) {
                        union(parent, i, j);
                    }
                }
            }
            Map<Integer, Integer> bestTransportSecondsByRoot = new HashMap<>();
            for (int i = 0; i < sameNameStops.size(); i++) {
                int root = find(parent, i);
                int transportSeconds = sameNameStops.get(i).transportSeconds;
                bestTransportSecondsByRoot.merge(root, transportSeconds, Math::min);
            }
            for (int i = 0; i < sameNameStops.size(); i++) {
                StopTime stop = sameNameStops.get(i);
                int bestTransportSeconds = bestTransportSecondsByRoot.get(find(parent, i));
                result.add(new StopTime(
                        stop.stopId,
                        stop.stopName,
                        stop.lat,
                        stop.lon,
                        bestTransportSeconds
                ));
            }
        }
        return result;
    }

    private static String normalizeStopName(String stopName) {
        return stopName == null ? "" : stopName.trim().toLowerCase(Locale.ROOT);
    }

    private static int find(int[] parent, int item) {
        int current = item;
        while (parent[current] != current) {
            parent[current] = parent[parent[current]];
            current = parent[current];
        }
        return current;
    }

    private static void union(int[] parent, int left, int right) {
        int leftRoot = find(parent, left);
        int rightRoot = find(parent, right);
        if (leftRoot != rightRoot) {
            parent[rightRoot] = leftRoot;
        }
    }

    private static BestAccess bestAccessOnRoadSegment(
            Point a,
            Point b,
            Point point,
            double segmentDistance,
            AccessLabel aAccess,
            AccessLabel bAccess
    ) {
        BestAccess best = null;
        if (aAccess != null) {
            double distanceFromA = haversineMeters(a.lat, a.lon, point.lat, point.lon);
            best = candidateAccess(best, aAccess, distanceFromA);
        }
        if (bAccess != null) {
            double distanceFromB = haversineMeters(b.lat, b.lon, point.lat, point.lon);
            best = candidateAccess(best, bAccess, Math.min(distanceFromB, segmentDistance));
        }
        return best;
    }

    private static BestAccess candidateAccess(BestAccess best, AccessLabel access, double extraMeters) {
        double walkMeters = access.walkMeters + extraMeters;
        if (walkMeters > WALK_RADIUS_METERS) {
            return best;
        }
        double totalSeconds = access.totalSeconds + extraMeters / WALK_SPEED_MPS;
        double walkSeconds = walkMeters / WALK_SPEED_MPS;
        if (best == null || totalSeconds < best.totalSeconds) {
            return new BestAccess(access.stop, walkMeters, walkSeconds, totalSeconds);
        }
        return best;
    }

    private static BestAccess nearestAccessOnRoadSegment(
            Point a,
            Point b,
            Point point,
            double segmentDistance,
            AccessLabel aAccess,
            AccessLabel bAccess
    ) {
        BestAccess best = null;
        if (aAccess != null) {
            double distanceFromA = haversineMeters(a.lat, a.lon, point.lat, point.lon);
            best = candidateNearestAccess(best, aAccess, distanceFromA);
        }
        if (bAccess != null) {
            double distanceFromB = haversineMeters(b.lat, b.lon, point.lat, point.lon);
            best = candidateNearestAccess(best, bAccess, Math.min(distanceFromB, segmentDistance));
        }
        return best;
    }

    private static BestAccess candidateNearestAccess(BestAccess best, AccessLabel access, double extraMeters) {
        double walkMeters = access.walkMeters + extraMeters;
        if (walkMeters > WALK_RADIUS_METERS) {
            return best;
        }
        double walkSeconds = walkMeters / WALK_SPEED_MPS;
        double totalSeconds = access.stop.transportSeconds + walkSeconds;
        if (best == null || walkMeters < best.walkDistanceMeters) {
            return new BestAccess(access.stop, walkMeters, walkSeconds, totalSeconds);
        }
        return best;
    }

    private static String pointKey(Point point) {
        return String.format(Locale.ROOT, "%.7f|%.7f", point.lat, point.lon);
    }

    private static ColorScale renderTiles(
            List<AccessibleSegment> segments,
            Path tileRoot,
            ColorMetric metric,
            Map<Integer, Map<String, List<AccessibleSegment>>> tileIndexes
    ) throws Exception {
        ColorScale colorScale = ColorScale.fromSegments(segments, metric);
        Path tempRoot = tileRoot.resolveSibling(tileRoot.getFileName() + ".tmp-" + System.currentTimeMillis());
        Files.createDirectories(tempRoot);
        try {
            for (int zoom = MIN_ZOOM; zoom <= MAX_ZOOM; zoom++) {
                Map<String, List<AccessibleSegment>> segmentsByTile = tileIndexes.getOrDefault(zoom, Map.of());
                segmentsByTile.entrySet().parallelStream().forEach(entry -> writeTileUnchecked(
                        tempRoot.resolve(entry.getKey() + ".png"),
                        renderTileSegments(entry.getKey(), entry.getValue(), colorScale)
                ));
            }
            if (Files.exists(tileRoot)) {
                clearDirectory(tileRoot);
                Files.delete(tileRoot);
            }
            Files.createDirectories(tileRoot.getParent());
            try {
                Files.move(tempRoot, tileRoot, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception ignored) {
                Files.move(tempRoot, tileRoot, StandardCopyOption.REPLACE_EXISTING);
            }
            return colorScale;
        } finally {
            if (Files.exists(tempRoot)) {
                clearDirectory(tempRoot);
                Files.deleteIfExists(tempRoot);
            }
        }
    }

    private static void renderContourTiles(
            Path tileRoot,
            Map<Integer, Map<String, List<AccessibleSegment>>> tileIndexes
    ) throws Exception {
        Path tempRoot = tileRoot.resolveSibling(tileRoot.getFileName() + ".tmp-" + System.currentTimeMillis());
        Files.createDirectories(tempRoot);
        try {
            for (int zoom = MIN_ZOOM; zoom <= MAX_ZOOM; zoom++) {
                Map<String, List<AccessibleSegment>> segmentsByTile = tileIndexes.getOrDefault(zoom, Map.of());
                segmentsByTile.entrySet().parallelStream().forEach(entry -> writeTileUnchecked(
                        tempRoot.resolve(entry.getKey() + ".png"),
                        renderTileContours(entry.getKey(), entry.getValue())
                ));
            }
            if (Files.exists(tileRoot)) {
                clearDirectory(tileRoot);
                Files.delete(tileRoot);
            }
            Files.createDirectories(tileRoot.getParent());
            try {
                Files.move(tempRoot, tileRoot, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception ignored) {
                Files.move(tempRoot, tileRoot, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            if (Files.exists(tempRoot)) {
                clearDirectory(tempRoot);
                Files.deleteIfExists(tempRoot);
            }
        }
    }

    private static Map<Integer, Map<String, List<AccessibleSegment>>> buildTileIndexes(
            List<AccessibleSegment> segments,
            double maxTotalMinutes
    ) {
        Map<Integer, Map<String, List<AccessibleSegment>>> indexes = new HashMap<>();
        for (int zoom = MIN_ZOOM; zoom <= MAX_ZOOM; zoom++) {
            Map<String, List<AccessibleSegment>> segmentsByTile = new HashMap<>();
            for (AccessibleSegment segment : segments) {
                if (segment.totalMinutes <= maxTotalMinutes) {
                    indexSegmentTiles(segmentsByTile, segment, zoom);
                }
            }
            indexes.put(zoom, segmentsByTile);
        }
        return indexes;
    }

    private static void writeTileUnchecked(Path tilePath, BufferedImage image) {
        try {
            Files.createDirectories(tilePath.getParent());
            writePngFast(image, tilePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write tile " + tilePath, e);
        }
    }

    private static void writePngFast(BufferedImage image, Path tilePath) throws Exception {
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("png");
        if (!writers.hasNext()) {
            ImageIO.write(image, "png", tilePath.toFile());
            return;
        }
        ImageWriter writer = writers.next();
        ImageWriteParam param = writer.getDefaultWriteParam();
        if (param.canWriteCompressed()) {
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(1.0f);
        }
        try (ImageOutputStream output = ImageIO.createImageOutputStream(tilePath.toFile())) {
            writer.setOutput(output);
            writer.write(null, new IIOImage(image, null, null), param);
        } finally {
            writer.dispose();
        }
    }

    private static void indexSegmentTiles(Map<String, List<AccessibleSegment>> segmentsByTile, AccessibleSegment segment, int zoom) {
        double ax = lonToPixelX(segment.from.lon, zoom);
        double ay = latToPixelY(segment.from.lat, zoom);
        double bx = lonToPixelX(segment.to.lon, zoom);
        double by = latToPixelY(segment.to.lat, zoom);
        int margin = Math.max(5, strokeWidth(zoom) + 2);
        int minTileX = (int) Math.floor((Math.min(ax, bx) - margin) / TILE_SIZE);
        int maxTileX = (int) Math.floor((Math.max(ax, bx) + margin) / TILE_SIZE);
        int minTileY = (int) Math.floor((Math.min(ay, by) - margin) / TILE_SIZE);
        int maxTileY = (int) Math.floor((Math.max(ay, by) + margin) / TILE_SIZE);
        for (int tileX = minTileX; tileX <= maxTileX; tileX++) {
            for (int tileY = minTileY; tileY <= maxTileY; tileY++) {
                String key = zoom + "/" + tileX + "/" + tileY;
                segmentsByTile.computeIfAbsent(key, ignored -> new ArrayList<>()).add(segment);
            }
        }
    }

    private static BufferedImage renderTileSegments(String key, List<AccessibleSegment> segments, ColorScale colorScale) {
        String[] parts = key.split("/");
        int zoom = Integer.parseInt(parts[0]);
        int tileX = Integer.parseInt(parts[1]);
        int tileY = Integer.parseInt(parts[2]);
        int offsetX = tileX * TILE_SIZE;
        int offsetY = tileY * TILE_SIZE;
        BufferedImage image = new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = image.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);
            g.setStroke(new BasicStroke(strokeWidth(zoom), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
            for (AccessibleSegment segment : segments) {
                double ax = lonToPixelX(segment.from.lon, zoom);
                double ay = latToPixelY(segment.from.lat, zoom);
                double bx = lonToPixelX(segment.to.lon, zoom);
                double by = latToPixelY(segment.to.lat, zoom);
                g.setColor(accessibilityColor(colorScale.metric.value(segment), colorScale));
                g.drawLine(
                        (int) Math.round(ax - offsetX),
                        (int) Math.round(ay - offsetY),
                        (int) Math.round(bx - offsetX),
                        (int) Math.round(by - offsetY)
                );
            }
        } finally {
            g.dispose();
        }
        return image;
    }

    private static BufferedImage renderTileContours(String key, List<AccessibleSegment> segments) {
        String[] parts = key.split("/");
        int zoom = Integer.parseInt(parts[0]);
        int tileX = Integer.parseInt(parts[1]);
        int tileY = Integer.parseInt(parts[2]);
        int offsetX = tileX * TILE_SIZE;
        int offsetY = tileY * TILE_SIZE;
        BufferedImage image = new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = image.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);
            drawContourThreshold(g, segments, zoom, offsetX, offsetY, 60.0, new Color(18, 49, 88, 190), contourStrokeWidth(zoom, 60.0));
            drawContourThreshold(g, segments, zoom, offsetX, offsetY, 30.0, new Color(255, 255, 255, 215), contourStrokeWidth(zoom, 30.0));
            drawContourThreshold(g, segments, zoom, offsetX, offsetY, 15.0, new Color(18, 18, 18, 230), contourStrokeWidth(zoom, 15.0));
        } finally {
            g.dispose();
        }
        return image;
    }

    private static void drawContourThreshold(
            Graphics2D g,
            List<AccessibleSegment> segments,
            int zoom,
            int offsetX,
            int offsetY,
            double thresholdMinutes,
            Color color,
            int width
    ) {
        g.setStroke(new BasicStroke(width, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
        g.setColor(color);
        for (AccessibleSegment segment : segments) {
            if (segment.totalMinutes > thresholdMinutes) {
                continue;
            }
            double ax = lonToPixelX(segment.from.lon, zoom);
            double ay = latToPixelY(segment.from.lat, zoom);
            double bx = lonToPixelX(segment.to.lon, zoom);
            double by = latToPixelY(segment.to.lat, zoom);
            g.drawLine(
                    (int) Math.round(ax - offsetX),
                    (int) Math.round(ay - offsetY),
                    (int) Math.round(bx - offsetX),
                    (int) Math.round(by - offsetY)
            );
        }
    }

    private static int contourStrokeWidth(int zoom, double thresholdMinutes) {
        int base = Math.max(2, strokeWidth(zoom) + 1);
        if (thresholdMinutes <= 15.0) {
            return base + 2;
        }
        if (thresholdMinutes <= 30.0) {
            return base + 1;
        }
        return base;
    }

    private static int strokeWidth(int zoom) {
        if (zoom <= 10) {
            return 2;
        }
        if (zoom <= 12) {
            return 3;
        }
        if (zoom <= 14) {
            return 4;
        }
        return 5;
    }

    private static Color accessibilityColor(double minutes, ColorScale colorScale) {
        double range = Math.max(1.0, colorScale.maxMinutes - colorScale.minMinutes);
        double n;
        if (colorScale.logScale) {
            n = Math.log1p(Math.max(0.0, minutes - colorScale.minMinutes)) / Math.log1p(range);
        } else {
            n = (minutes - colorScale.minMinutes) / range;
        }
        n = Math.max(0, Math.min(1, n));
        int[] a;
        int[] b;
        double t;
        if (n < 0.34) {
            a = new int[]{24, 134, 75};
            b = new int[]{213, 200, 61};
            t = n / 0.34;
        } else if (n < 0.68) {
            a = new int[]{213, 200, 61};
            b = new int[]{217, 92, 43};
            t = (n - 0.34) / 0.34;
        } else {
            a = new int[]{217, 92, 43};
            b = new int[]{125, 33, 52};
            t = (n - 0.68) / 0.32;
        }
        return new Color(
                (int) Math.round(a[0] + (b[0] - a[0]) * t),
                (int) Math.round(a[1] + (b[1] - a[1]) * t),
                (int) Math.round(a[2] + (b[2] - a[2]) * t),
                210
        );
    }

    private static void clearDirectory(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        List<Path> paths;
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        for (Path path : paths) {
            if (!path.equals(dir)) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static ContourStats calculateContourStats(List<AccessibleSegment> segments) {
        double area15 = contourAreaSquareKm(segments, 15.0);
        double area30 = contourAreaSquareKm(segments, 30.0);
        double area60 = contourAreaSquareKm(segments, 60.0);
        return new ContourStats(area15, area30, area60);
    }

    private static double contourAreaSquareKm(List<AccessibleSegment> segments, double thresholdMinutes) {
        double squareMeters = segments.stream()
                .filter(segment -> segment.totalMinutes <= thresholdMinutes)
                .mapToDouble(segment -> segment.lengthMeters * SAMPLE_METERS)
                .sum();
        return squareMeters / 1_000_000.0;
    }

    private static void writeContourStats(SparkSession spark, Path outputDir, List<SnapshotPayload> snapshots) {
        try {
            Files.createDirectories(outputDir);
            List<Row> rows = snapshots.stream()
                    .map(snapshot -> RowFactory.create(
                            java.sql.Date.valueOf(snapshot.serviceDate),
                            snapshot.departureTime.toString(),
                            Timestamp.from(snapshot.serviceDate.atTime(snapshot.departureTime).atZone(CITY_ZONE).toInstant()),
                            snapshot.id,
                            snapshot.reachability.stopTimes.size(),
                            snapshot.segmentCount,
                            snapshot.contourStats.area15SquareKm,
                            snapshot.contourStats.area30SquareKm,
                            snapshot.contourStats.area60SquareKm
                    ))
                    .collect(Collectors.toList());
            spark.createDataFrame(rows, contourStatsSchema())
                    .coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .parquet(outputDir.toAbsolutePath().toString());
        } catch (Exception e) {
            System.err.println("BusAccessibilityMapCacheJob: failed to write contour stats: " + e.getMessage());
        }
    }

    private static StructType contourStatsSchema() {
        return new StructType(new StructField[]{
                new StructField("serviceDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("departureTime", DataTypes.StringType, false, Metadata.empty()),
                new StructField("departureAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("snapshotId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("reachableStops", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("segmentCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("area15SquareKm", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("area30SquareKm", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("area60SquareKm", DataTypes.DoubleType, false, Metadata.empty())
        });
    }

    private static void writePayload(
            Path outputFile,
            Path tileBaseRoot,
            String originStopQuery,
            String sourceMode,
            List<RoadWay> roads,
            List<LocalDate> requestedServiceDates,
            List<LocalTime> requestedDepartureTimes,
            List<SnapshotPayload> snapshots
    ) throws Exception {
        if (snapshots.isEmpty()) {
            throw new IllegalStateException("No accessibility snapshots were generated");
        }
        SnapshotPayload first = snapshots.get(0);
        Reachability reachability = first.reachability;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().toString());
        payload.put("sourceMode", sourceMode);
        payload.put("serviceDate", first.serviceDate.toString());
        payload.put("departureTime", first.departureTime.toString());
        payload.put("originStopQuery", originStopQuery);
        payload.put("originStops", reachability.originStops.stream().map(BusAccessibilityMapCacheJob::stopPayload).collect(Collectors.toList()));
        payload.put("originStopName", reachability.originStops.stream().map(stop -> stop.stopName).distinct().collect(Collectors.joining(", ")));
        payload.put("originLatitude", reachability.originStops.stream().mapToDouble(stop -> stop.lat).average().orElse(55.79));
        payload.put("originLongitude", reachability.originStops.stream().mapToDouble(stop -> stop.lon).average().orElse(49.12));
        payload.put("walkSpeedMps", WALK_SPEED_MPS);
        payload.put("walkRadiusMeters", WALK_RADIUS_METERS);
        payload.put("stopClusterRadiusMeters", STOP_CLUSTER_RADIUS_METERS);
        payload.put("sampleMeters", SAMPLE_METERS);
        payload.put("maxRides", MAX_RIDES);
        payload.put("maxCandidateEventsPerStop", MAX_CANDIDATE_EVENTS_PER_STOP);
        payload.put("reachableStops", reachability.stopTimes.size());
        payload.put("roadWays", roads.size());
        payload.put("segmentCount", first.segmentCount);
        payload.put("tileMinZoom", MIN_ZOOM);
        payload.put("tileMaxZoom", MAX_ZOOM);
        payload.put("tileOverlayTemplate", first.tileOverlayTemplate());
        payload.put("contourTileOverlayTemplate", first.contourTileOverlayTemplate());
        payload.put("totalFullTileOverlayTemplate", first.totalFullColorScale == null ? null : first.totalFullTileOverlayTemplate());
        payload.put("totalLogTileOverlayTemplate", first.totalLogColorScale == null ? null : first.totalLogTileOverlayTemplate());
        payload.put("walkTileOverlayTemplate", first.walkColorScale == null ? null : first.walkTileOverlayTemplate());
        payload.put("stopTransportTileOverlayTemplate", first.stopTransportColorScale == null ? null : first.stopTransportTileOverlayTemplate());
        payload.put("tileRoot", tileBaseRoot.resolve(first.id).resolve("total").toString());
        payload.put("totalFullTileRoot", tileBaseRoot.resolve(first.id).resolve("total-full").toString());
        payload.put("totalLogTileRoot", tileBaseRoot.resolve(first.id).resolve("total-log").toString());
        payload.put("walkTileRoot", tileBaseRoot.resolve(first.id).resolve("walk").toString());
        payload.put("stopTransportTileRoot", tileBaseRoot.resolve(first.id).resolve("stop-transport").toString());
        payload.put("contourThresholdMinutes", List.of(15, 30, 60));
        payload.put("totalColorMinMinutes", round1(first.totalColorScale.minMinutes));
        payload.put("totalColorMaxMinutes", round1(first.totalColorScale.maxMinutes));
        putScale(payload, "totalFull", first.totalFullColorScale);
        putScale(payload, "totalLog", first.totalLogColorScale);
        putScale(payload, "walk", first.walkColorScale);
        putScale(payload, "stopTransport", first.stopTransportColorScale);
        payload.put("stops", reachability.stopTimes.stream().map(BusAccessibilityMapCacheJob::stopTimePayload).collect(Collectors.toList()));
        payload.put("serviceDates", snapshots.stream().map(snapshot -> snapshot.serviceDate.toString()).distinct().collect(Collectors.toList()));
        payload.put("departureTimes", snapshots.stream().map(snapshot -> snapshot.departureTime.toString()).distinct().collect(Collectors.toList()));
        payload.put("requestedServiceDates", requestedServiceDates.stream().map(LocalDate::toString).collect(Collectors.toList()));
        payload.put("requestedDepartureTimes", requestedDepartureTimes.stream().map(LocalTime::toString).collect(Collectors.toList()));
        payload.put("availableModes", first.availableModes());
        payload.put("contourAreaSeries", snapshots.stream().map(BusAccessibilityMapCacheJob::contourAreaPayload).collect(Collectors.toList()));
        payload.put("snapshots", snapshots.stream().map(BusAccessibilityMapCacheJob::snapshotPayload).collect(Collectors.toList()));
        writeJsonAtomic(outputFile, payload);
    }

    private static Map<String, Object> snapshotPayload(SnapshotPayload snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", snapshot.id);
        payload.put("serviceDate", snapshot.serviceDate.toString());
        payload.put("departureTime", snapshot.departureTime.toString());
        payload.put("reachableStops", snapshot.reachability.stopTimes.size());
        payload.put("segmentCount", snapshot.segmentCount);
        payload.put("tileOverlayTemplate", snapshot.tileOverlayTemplate());
        payload.put("contourTileOverlayTemplate", snapshot.contourTileOverlayTemplate());
        payload.put("availableModes", snapshot.availableModes());
        payload.put("totalFullTileOverlayTemplate", snapshot.totalFullColorScale == null ? null : snapshot.totalFullTileOverlayTemplate());
        payload.put("totalLogTileOverlayTemplate", snapshot.totalLogColorScale == null ? null : snapshot.totalLogTileOverlayTemplate());
        payload.put("walkTileOverlayTemplate", snapshot.walkColorScale == null ? null : snapshot.walkTileOverlayTemplate());
        payload.put("stopTransportTileOverlayTemplate", snapshot.stopTransportColorScale == null ? null : snapshot.stopTransportTileOverlayTemplate());
        payload.put("totalColorMinMinutes", round1(snapshot.totalColorScale.minMinutes));
        payload.put("totalColorMaxMinutes", round1(snapshot.totalColorScale.maxMinutes));
        putScale(payload, "totalFull", snapshot.totalFullColorScale);
        putScale(payload, "totalLog", snapshot.totalLogColorScale);
        putScale(payload, "walk", snapshot.walkColorScale);
        putScale(payload, "stopTransport", snapshot.stopTransportColorScale);
        payload.put("contourArea", contourAreaPayload(snapshot));
        return payload;
    }

    private static Map<String, Object> contourAreaPayload(SnapshotPayload snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("snapshotId", snapshot.id);
        payload.put("serviceDate", snapshot.serviceDate.toString());
        payload.put("departureTime", snapshot.departureTime.toString());
        payload.put("area15SquareKm", round2(snapshot.contourStats.area15SquareKm));
        payload.put("area30SquareKm", round2(snapshot.contourStats.area30SquareKm));
        payload.put("area60SquareKm", round2(snapshot.contourStats.area60SquareKm));
        return payload;
    }

    private static void putScale(Map<String, Object> payload, String prefix, ColorScale scale) {
        if (scale == null) {
            return;
        }
        payload.put(prefix + "ColorMinMinutes", round1(scale.minMinutes));
        payload.put(prefix + "ColorMaxMinutes", round1(scale.maxMinutes));
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static Map<String, Object> stopPayload(StopPoint stop) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("stopId", stop.stopId);
        payload.put("stopName", stop.stopName);
        payload.put("latitude", stop.lat);
        payload.put("longitude", stop.lon);
        return payload;
    }

    private static Map<String, Object> stopTimePayload(StopTime stop) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("stopId", stop.stopId);
        payload.put("stopName", stop.stopName);
        payload.put("latitude", stop.lat);
        payload.put("longitude", stop.lon);
        payload.put("transportSeconds", stop.transportSeconds);
        payload.put("transportMinutes", Math.round(stop.transportSeconds / 6.0) / 10.0);
        return payload;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Files.createDirectories(targetFile.getParent());
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Point interpolate(Point a, Point b, double t) {
        return new Point(a.lat + (b.lat - a.lat) * t, a.lon + (b.lon - a.lon) * t);
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double earthRadius = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadius * c;
    }

    private static double lonToPixelX(double lon, int zoom) {
        return ((lon + 180.0) / 360.0) * TILE_SIZE * Math.pow(2, zoom);
    }

    private static double latToPixelY(double lat, int zoom) {
        double sin = Math.sin(Math.toRadians(lat));
        return (0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI)) * TILE_SIZE * Math.pow(2, zoom);
    }

    private static int secondsOfDay(Instant instant, LocalDate serviceDate) {
        LocalDateTime local = LocalDateTime.ofInstant(instant, CITY_ZONE);
        return (int) (java.time.Duration.between(serviceDate.atStartOfDay(), local).getSeconds());
    }

    private static final class Event {
        private final String tripId;
        private final String plate;
        private final String internalRouteId;
        private final String routeNumber;
        private final String segmentId;
        private final String startStopId;
        private final String endStopId;
        private final int departureSecond;
        private final int arrivalSecond;

        private Event(
                String tripId,
                String plate,
                String internalRouteId,
                String routeNumber,
                String segmentId,
                String startStopId,
                String endStopId,
                int departureSecond,
                int arrivalSecond
        ) {
            this.tripId = tripId;
            this.plate = plate;
            this.internalRouteId = internalRouteId;
            this.routeNumber = routeNumber;
            this.segmentId = segmentId;
            this.startStopId = startStopId;
            this.endStopId = endStopId;
            this.departureSecond = departureSecond;
            this.arrivalSecond = arrivalSecond;
        }

        private static Event fromRow(Row row, LocalDate serviceDate) {
            Object start = row.getAs("startExitedStopAt");
            Object end = row.getAs("endEnteredStopAt");
            Instant startInstant = instantValue(start);
            Instant endInstant = instantValue(end);
            if (startInstant == null || endInstant == null) {
                return null;
            }
            int departureSecond = secondsOfDay(startInstant, serviceDate);
            int arrivalSecond = secondsOfDay(endInstant, serviceDate);
            Long travelDurationSeconds = longValue(row.getAs("travelDurationSeconds"));
            if (arrivalSecond <= departureSecond || travelDurationSeconds == null || travelDurationSeconds <= 0) {
                return null;
            }
            return new Event(
                    stringValue(row.getAs("tripId")),
                    stringValue(row.getAs("plate")),
                    stringValue(row.getAs("internalRouteId")),
                    stringValue(row.getAs("routeNumber")),
                    stringValue(row.getAs("segmentId")),
                    stringValue(row.getAs("startStopId")),
                    stringValue(row.getAs("endStopId")),
                    departureSecond,
                    arrivalSecond
            );
        }
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static Instant instantValue(Object value) {
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(CITY_ZONE).toInstant();
        }
        if (value instanceof Instant) {
            return (Instant) value;
        }
        return null;
    }

    private static Long longValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private static final class StateNode {
        private final String stopId;
        private final int arrivalSecond;
        private final String rideKey;
        private final int rideCount;

        private StateNode(String stopId, int arrivalSecond, String rideKey, int rideCount) {
            this.stopId = stopId;
            this.arrivalSecond = arrivalSecond;
            this.rideKey = rideKey;
            this.rideCount = rideCount;
        }
    }

    private static final class Point {
        private final double lat;
        private final double lon;

        private Point(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    private static final class RoadWay {
        private final long id;
        private final String highway;
        private final String name;
        private final List<Point> points;

        private RoadWay(long id, String highway, String name, List<Point> points) {
            this.id = id;
            this.highway = highway;
            this.name = name;
            this.points = points;
        }
    }

    private static final class AccessibleSegment {
        private final Point from;
        private final Point to;
        private final double totalMinutes;
        private final double walkMinutes;
        private final double walkDistanceMeters;
        private final double stopTransportMinutes;
        private final double nearestStopTransportMinutes;
        private final double nearestStopWalkDistanceMeters;
        private final double lengthMeters;
        private final String nearestStopId;
        private final String nearestStopName;

        private AccessibleSegment(
                Point from,
                Point to,
                double totalMinutes,
                double walkMinutes,
                double walkDistanceMeters,
                double stopTransportMinutes,
                double nearestStopTransportMinutes,
                double nearestStopWalkDistanceMeters,
                double lengthMeters,
                String nearestStopId,
                String nearestStopName
        ) {
            this.from = from;
            this.to = to;
            this.totalMinutes = totalMinutes;
            this.walkMinutes = walkMinutes;
            this.walkDistanceMeters = walkDistanceMeters;
            this.stopTransportMinutes = stopTransportMinutes;
            this.nearestStopTransportMinutes = nearestStopTransportMinutes;
            this.nearestStopWalkDistanceMeters = nearestStopWalkDistanceMeters;
            this.lengthMeters = lengthMeters;
            this.nearestStopId = nearestStopId;
            this.nearestStopName = nearestStopName;
        }
    }

    private static final class StopTime {
        private final String stopId;
        private final String stopName;
        private final double lat;
        private final double lon;
        private final int transportSeconds;

        private StopTime(String stopId, String stopName, double lat, double lon, int transportSeconds) {
            this.stopId = stopId;
            this.stopName = stopName;
            this.lat = lat;
            this.lon = lon;
            this.transportSeconds = transportSeconds;
        }
    }

    private static final class StopPoint {
        private final String stopId;
        private final String stopName;
        private final double lat;
        private final double lon;

        private StopPoint(String stopId, String stopName, double lat, double lon) {
            this.stopId = stopId;
            this.stopName = stopName;
            this.lat = lat;
            this.lon = lon;
        }
    }

    private static final class Reachability {
        private final List<StopPoint> originStops;
        private final List<StopTime> stopTimes;

        private Reachability(List<StopPoint> originStops, List<StopTime> stopTimes) {
            this.originStops = originStops;
            this.stopTimes = stopTimes;
        }
    }

    private static final class RoadGraph {
        private final List<Point> nodes;
        private final Map<String, Integer> nodeIdsByPointKey;
        private final Map<Integer, List<RoadEdge>> edgesByNode;
        private final Map<String, List<Integer>> nodeIdsByGridCell;

        private RoadGraph(
                List<Point> nodes,
                Map<String, Integer> nodeIdsByPointKey,
                Map<Integer, List<RoadEdge>> edgesByNode,
                Map<String, List<Integer>> nodeIdsByGridCell
        ) {
            this.nodes = nodes;
            this.nodeIdsByPointKey = nodeIdsByPointKey;
            this.edgesByNode = edgesByNode;
            this.nodeIdsByGridCell = nodeIdsByGridCell;
        }

        private static RoadGraph build(List<RoadWay> roads) {
            List<Point> nodes = new ArrayList<>();
            Map<String, Integer> nodeIdsByPointKey = new HashMap<>();
            Map<Integer, List<RoadEdge>> edgesByNode = new HashMap<>();
            Map<String, List<Integer>> nodeIdsByGridCell = new HashMap<>();
            for (RoadWay road : roads) {
                for (int i = 1; i < road.points.size(); i++) {
                    Point a = road.points.get(i - 1);
                    Point b = road.points.get(i);
                    int aId = nodeId(a, nodes, nodeIdsByPointKey, nodeIdsByGridCell);
                    int bId = nodeId(b, nodes, nodeIdsByPointKey, nodeIdsByGridCell);
                    if (aId == bId) {
                        continue;
                    }
                    double distance = haversineMeters(a.lat, a.lon, b.lat, b.lon);
                    if (distance <= 0.0) {
                        continue;
                    }
                    edgesByNode.computeIfAbsent(aId, ignored -> new ArrayList<>()).add(new RoadEdge(bId, distance));
                    edgesByNode.computeIfAbsent(bId, ignored -> new ArrayList<>()).add(new RoadEdge(aId, distance));
                }
            }
            return new RoadGraph(nodes, nodeIdsByPointKey, edgesByNode, nodeIdsByGridCell);
        }

        private static int nodeId(
                Point point,
                List<Point> nodes,
                Map<String, Integer> nodeIdsByPointKey,
                Map<String, List<Integer>> nodeIdsByGridCell
        ) {
            String key = pointKey(point);
            Integer existing = nodeIdsByPointKey.get(key);
            if (existing != null) {
                return existing;
            }
            int id = nodes.size();
            nodes.add(point);
            nodeIdsByPointKey.put(key, id);
            nodeIdsByGridCell.computeIfAbsent(gridCellKey(point.lat, point.lon), ignored -> new ArrayList<>()).add(id);
            return id;
        }

        private List<NodeDistance> closestNodes(double lat, double lon, double radiusMeters, int fallbackCount) {
            List<NodeDistance> withinRadius = new ArrayList<>();
            PriorityQueue<NodeDistance> nearest = new PriorityQueue<>((left, right) -> Double.compare(right.distanceMeters, left.distanceMeters));
            int centerLatCell = gridCell(lat);
            int centerLonCell = gridCell(lon);
            int radiusCells = Math.max(1, (int) Math.ceil(radiusMeters / 450.0) + 1);
            Set<Integer> scannedNodeIds = new HashSet<>();
            for (int latCell = centerLatCell - radiusCells; latCell <= centerLatCell + radiusCells; latCell++) {
                for (int lonCell = centerLonCell - radiusCells; lonCell <= centerLonCell + radiusCells; lonCell++) {
                    for (Integer nodeId : nodeIdsByGridCell.getOrDefault(gridCellKey(latCell, lonCell), List.of())) {
                        if (scannedNodeIds.add(nodeId)) {
                            addCandidateNode(lat, lon, radiusMeters, fallbackCount, withinRadius, nearest, nodeId);
                        }
                    }
                }
            }
            if (withinRadius.isEmpty() && nearest.size() < fallbackCount) {
                for (int i = 0; i < nodes.size(); i++) {
                    if (scannedNodeIds.add(i)) {
                        addCandidateNode(lat, lon, radiusMeters, fallbackCount, withinRadius, nearest, i);
                    }
                }
            }
            if (!withinRadius.isEmpty()) {
                withinRadius.sort(Comparator.comparingDouble(item -> item.distanceMeters));
                return withinRadius;
            }
            List<NodeDistance> fallback = new ArrayList<>(nearest);
            fallback.sort(Comparator.comparingDouble(item -> item.distanceMeters));
            return fallback;
        }

        private void addCandidateNode(
                double lat,
                double lon,
                double radiusMeters,
                int fallbackCount,
                List<NodeDistance> withinRadius,
                PriorityQueue<NodeDistance> nearest,
                int nodeId
        ) {
            Point node = nodes.get(nodeId);
            double distance = haversineMeters(lat, lon, node.lat, node.lon);
            if (distance <= radiusMeters) {
                withinRadius.add(new NodeDistance(nodeId, distance));
            }
            if (fallbackCount > 0) {
                nearest.add(new NodeDistance(nodeId, distance));
                if (nearest.size() > fallbackCount) {
                    nearest.poll();
                }
            }
        }

        private static int gridCell(double value) {
            return (int) Math.floor(value / ROAD_NODE_GRID_DEGREES);
        }

        private static String gridCellKey(double lat, double lon) {
            return gridCell(lat) + "|" + gridCell(lon);
        }

        private static String gridCellKey(int latCell, int lonCell) {
            return latCell + "|" + lonCell;
        }
    }

    private static final class RoadEdge {
        private final int toNodeId;
        private final double distanceMeters;

        private RoadEdge(int toNodeId, double distanceMeters) {
            this.toNodeId = toNodeId;
            this.distanceMeters = distanceMeters;
        }
    }

    private static final class NodeDistance {
        private final int nodeId;
        private final double distanceMeters;

        private NodeDistance(int nodeId, double distanceMeters) {
            this.nodeId = nodeId;
            this.distanceMeters = distanceMeters;
        }
    }

    private static final class AccessState {
        private final int nodeId;
        private final double totalSeconds;
        private final double walkMeters;
        private final StopTime stop;

        private AccessState(int nodeId, double totalSeconds, double walkMeters, StopTime stop) {
            this.nodeId = nodeId;
            this.totalSeconds = totalSeconds;
            this.walkMeters = walkMeters;
            this.stop = stop;
        }
    }

    private static final class AccessLabel {
        private final StopTime stop;
        private final double totalSeconds;
        private final double walkMeters;

        private AccessLabel(StopTime stop, double totalSeconds, double walkMeters) {
            this.stop = stop;
            this.totalSeconds = totalSeconds;
            this.walkMeters = walkMeters;
        }
    }

    private static final class ColorScale {
        private final ColorMetric metric;
        private final double minMinutes;
        private final double maxMinutes;
        private final boolean logScale;

        private ColorScale(ColorMetric metric, double minMinutes, double maxMinutes) {
            this(metric, minMinutes, maxMinutes, false);
        }

        private ColorScale(ColorMetric metric, double minMinutes, double maxMinutes, boolean logScale) {
            this.metric = metric;
            this.minMinutes = minMinutes;
            this.maxMinutes = maxMinutes;
            this.logScale = logScale;
        }

        private static ColorScale fromSegments(List<AccessibleSegment> segments, ColorMetric metric) {
            if (metric == ColorMetric.WALK) {
                return new ColorScale(metric, 0.0, WALK_RADIUS_METERS / WALK_SPEED_MPS / 60.0);
            }
            if (COLOR_MAX_MINUTES > COLOR_MIN_MINUTES) {
                return new ColorScale(metric, COLOR_MIN_MINUTES, COLOR_MAX_MINUTES);
            }
            if (segments.isEmpty()) {
                return new ColorScale(metric, 0.0, 120.0);
            }
            List<Double> values = segments.stream()
                    .map(metric::value)
                    .filter(value -> Double.isFinite(value) && value >= 0)
                    .sorted()
                    .collect(Collectors.toList());
            if (values.isEmpty()) {
                return new ColorScale(metric, 0.0, 120.0);
            }
            double min = COLOR_MIN_MINUTES > 0 ? COLOR_MIN_MINUTES : values.get(0);
            double max;
            if (metric == ColorMetric.TOTAL_FULL || metric == ColorMetric.TOTAL_LOG) {
                max = values.get(values.size() - 1);
            } else {
                int percentileIndex = (int) Math.floor(Math.max(0.0, Math.min(1.0, COLOR_MAX_PERCENTILE)) * (values.size() - 1));
                max = values.get(percentileIndex);
            }
            if (max <= min + 1.0) {
                max = min + 120.0;
            }
            return new ColorScale(metric, min, max, metric == ColorMetric.TOTAL_LOG);
        }
    }

    private static final class SnapshotPayload {
        private final String id;
        private final LocalDate serviceDate;
        private final LocalTime departureTime;
        private final Reachability reachability;
        private final int segmentCount;
        private final ContourStats contourStats;
        private final ColorScale totalColorScale;
        private final ColorScale totalFullColorScale;
        private final ColorScale totalLogColorScale;
        private final ColorScale walkColorScale;
        private final ColorScale stopTransportColorScale;

        private SnapshotPayload(
                String id,
                LocalDate serviceDate,
                LocalTime departureTime,
                Reachability reachability,
                int segmentCount,
                ContourStats contourStats,
                ColorScale totalColorScale,
                ColorScale totalFullColorScale,
                ColorScale totalLogColorScale,
                ColorScale walkColorScale,
                ColorScale stopTransportColorScale
        ) {
            this.id = id;
            this.serviceDate = serviceDate;
            this.departureTime = departureTime;
            this.reachability = reachability;
            this.segmentCount = segmentCount;
            this.contourStats = contourStats;
            this.totalColorScale = totalColorScale;
            this.totalFullColorScale = totalFullColorScale;
            this.totalLogColorScale = totalLogColorScale;
            this.walkColorScale = walkColorScale;
            this.stopTransportColorScale = stopTransportColorScale;
        }

        private String tileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/total/{z}/{x}/{y}.png";
        }

        private String contourTileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/contours/{z}/{x}/{y}.png";
        }

        private String totalFullTileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/total-full/{z}/{x}/{y}.png";
        }

        private String totalLogTileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/total-log/{z}/{x}/{y}.png";
        }

        private String walkTileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/walk/{z}/{x}/{y}.png";
        }

        private String stopTransportTileOverlayTemplate() {
            return "./tiles/accessibility/" + id + "/stop-transport/{z}/{x}/{y}.png";
        }

        private List<String> availableModes() {
            List<String> modes = new ArrayList<>();
            modes.add("total");
            if (totalFullColorScale != null) {
                modes.add("totalFull");
            }
            if (totalLogColorScale != null) {
                modes.add("totalLog");
            }
            if (walkColorScale != null) {
                modes.add("walk");
            }
            if (stopTransportColorScale != null) {
                modes.add("stopTransport");
            }
            return modes;
        }
    }

    private static final class ContourStats {
        private final double area15SquareKm;
        private final double area30SquareKm;
        private final double area60SquareKm;

        private ContourStats(double area15SquareKm, double area30SquareKm, double area60SquareKm) {
            this.area15SquareKm = area15SquareKm;
            this.area30SquareKm = area30SquareKm;
            this.area60SquareKm = area60SquareKm;
        }
    }

    private enum ColorMetric {
        TOTAL {
            @Override
            double value(AccessibleSegment segment) {
                return segment.totalMinutes;
            }
        },
        TOTAL_FULL {
            @Override
            double value(AccessibleSegment segment) {
                return segment.totalMinutes;
            }
        },
        TOTAL_LOG {
            @Override
            double value(AccessibleSegment segment) {
                return segment.totalMinutes;
            }
        },
        WALK {
            @Override
            double value(AccessibleSegment segment) {
                return segment.walkMinutes;
            }
        },
        STOP_TRANSPORT {
            @Override
            double value(AccessibleSegment segment) {
                return segment.nearestStopTransportMinutes;
            }
        };

        abstract double value(AccessibleSegment segment);
    }

    private static final class BestAccess {
        private final StopTime stop;
        private final double walkDistanceMeters;
        private final double walkSeconds;
        private final double totalSeconds;

        private BestAccess(StopTime stop, double walkDistanceMeters, double walkSeconds, double totalSeconds) {
            this.stop = stop;
            this.walkDistanceMeters = walkDistanceMeters;
            this.walkSeconds = walkSeconds;
            this.totalSeconds = totalSeconds;
        }
    }
}
