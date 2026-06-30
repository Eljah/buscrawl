import com.fasterxml.jackson.core.type.TypeReference;
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
import java.awt.image.IndexColorModel;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import java.util.ArrayDeque;
import java.util.Arrays;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

public class BusAccessibilityMapCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int WALK_CACHE_MAGIC = 0x42415731; // BAW1
    private static final int WALK_CACHE_VERSION = 1;
    private static final int PRIMITIVE_WALK_CACHE_MAGIC = 0x42415032; // BAP2
    private static final int PRIMITIVE_WALK_CACHE_VERSION = 1;
    private static final int RENDER_CACHE_MAGIC = 0x42415231; // BAR1
    private static final int RENDER_CACHE_VERSION = 1;
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
    private static final int OVERLAY_MAX_ZOOM = Integer.parseInt(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_OVERLAY_TILE_MAX_ZOOM",
            String.valueOf(Math.min(MAX_ZOOM, 12))
    ));
    private static final boolean INDEXED_PNG = Boolean.parseBoolean(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_INDEXED_PNG",
            "true"
    ));
    private static final boolean PARALLEL_SEGMENT_BUILD = Boolean.parseBoolean(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_PARALLEL_SEGMENT_BUILD",
            "false"
    ));
    private static final boolean PRIMITIVE_WALK_CACHE = Boolean.parseBoolean(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_PRIMITIVE_WALK_CACHE",
            "true"
    ));
    private static final float PNG_COMPRESSION_QUALITY = Float.parseFloat(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_PNG_COMPRESSION_QUALITY",
            "0.0"
    ));
    private static final double COLOR_MIN_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MIN_MINUTES", "0"));
    private static final double COLOR_MAX_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MAX_MINUTES", "0"));
    private static final double COLOR_MAX_PERCENTILE = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_COLOR_MAX_PERCENTILE", "0.90"));
    private static final double TOTAL_NORMALIZED_COLOR_MAX_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_TOTAL_NORMALIZED_MAX_MINUTES", "800"));
    private static final double TOTAL_LOG_COLOR_MAX_MINUTES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_TOTAL_LOG_MAX_MINUTES", "800"));
    private static final double[] CONTOUR_THRESHOLDS_MINUTES = new double[]{15.0, 30.0, 60.0};
    private static final double CONTOUR_ENVELOPE_METERS = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_CONTOUR_ENVELOPE_METERS", "250"));
    private static final double CONTOUR_PROJECTION_LAT = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_CONTOUR_PROJECTION_LAT", "55.8"));
    private static final double EFFECTIVE_WALK_RADIUS_METERS = Math.max(
            WALK_RADIUS_METERS,
            CONTOUR_THRESHOLDS_MINUTES[CONTOUR_THRESHOLDS_MINUTES.length - 1] * 60.0 * WALK_SPEED_MPS
    );
    private static final double ROAD_NODE_GRID_DEGREES = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_ROAD_NODE_GRID_DEGREES", "0.005"));
    private static final int TILE_SIZE = 256;

    public static void main(String[] args) throws Exception {
        ImageIO.setUseCache(false);
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
        String tileUrlPrefix = normalizeTileUrlPrefix(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_TILE_URL_PREFIX",
                "accessibility"
        ));
        Path contourStatsDir = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_CONTOUR_STATS_DIR",
                outputFile.getParent().resolve("accessibility-contour-stats").toString()
        ));
        String originSlug = System.getenv().getOrDefault("BUS_ACCESSIBILITY_ORIGIN_SLUG", "").trim();
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
        Path walkCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_WALK_CACHE_FILE",
                outputFile.getParent().resolve("accessibility-walk-cache.bin.gz").toString()
        ));
        Path renderCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_RENDER_CACHE_FILE",
                outputFile.getParent().resolve("accessibility-render-cache.bin.gz").toString()
        ));
        Path householdsFile = Path.of(System.getenv().getOrDefault(
                "BUS_ACCESSIBILITY_HOUSEHOLDS_FILE",
                "/home/kazanparking/kazan-parking-local/osm-cache/codificator-geo.json.bak.20260617082853"
        ));
        if (Boolean.parseBoolean(System.getenv().getOrDefault("BUS_ACCESSIBILITY_BUILD_WALK_CACHE", "false"))) {
            Map<String, StopPoint> stopsById = loadStopsById();
            List<RoadWay> roads = loadRoadWays(osmRoadsFile);
            RoadGraph roadGraph = RoadGraph.build(roads);
            buildAndWriteWalkCache(walkCacheFile, roadGraph, roads, stopsById);
            return;
        }
        if (Boolean.parseBoolean(System.getenv().getOrDefault("BUS_ACCESSIBILITY_BUILD_RENDER_CACHE", "false"))) {
            WalkCache walkCache = readWalkCache(walkCacheFile);
            if (walkCache.primitive == null) {
                throw new IllegalStateException("Render cache requires primitive walk cache: " + walkCacheFile);
            }
            buildAndWriteRenderCache(renderCacheFile, walkCache.primitive);
            return;
        }

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
            WalkCache walkCache = Files.exists(walkCacheFile) ? readWalkCache(walkCacheFile) : null;
            RenderCache renderCache = Files.exists(renderCacheFile) ? readRenderCache(renderCacheFile) : null;
            HouseholdIndex householdIndex = loadHouseholdIndex(householdsFile);
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
                    List<AccessibleSegment> segments = walkCache == null
                            ? buildAccessibilitySegments(roadGraph, roads, reachability.stopTimes)
                            : buildAccessibilitySegments(walkCache, reachability.stopTimes);
                    String snapshotId = snapshotId(snapshotDate, snapshotTime);
                    debugTopTotalSegments(snapshotDate, snapshotTime, segments);
                    Path snapshotRoot = tileBaseRoot.resolve(snapshotId);
                    Path totalTileRoot = snapshotRoot.resolve("total");
                    Path totalNormalizedTileRoot = snapshotRoot.resolve("total-normalized");
                    Path totalLogTileRoot = snapshotRoot.resolve("total-log");
                    Path walkTileRoot = snapshotRoot.resolve("walk");
                    Path stopTransportTileRoot = snapshotRoot.resolve("stop-transport");
                    Map<Integer, Map<String, List<AccessibleSegment>>> tileIndexes = renderCache == null
                            ? buildTileIndexes(segments, Double.POSITIVE_INFINITY)
                            : Map.of();
                    logSnapshotStage(snapshotId, "render total tiles start");
                    ColorScale totalColorScale = renderTiles(segments, totalTileRoot, ColorMetric.TOTAL, tileIndexes, renderCache);
                    logSnapshotStage(snapshotId, "render total tiles done");
                    ColorScale totalNormalizedColorScale = null;
                    if (renderModes.contains("totalNormalized")) {
                        logSnapshotStage(snapshotId, "render totalNormalized tiles start");
                        totalNormalizedColorScale = renderTiles(segments, totalNormalizedTileRoot, ColorMetric.TOTAL_NORMALIZED, tileIndexes, renderCache);
                        logSnapshotStage(snapshotId, "render totalNormalized tiles done");
                    }
                    ColorScale totalLogColorScale = null;
                    if (renderModes.contains("totalLog")) {
                        logSnapshotStage(snapshotId, "render totalLog tiles start");
                        totalLogColorScale = renderTiles(segments, totalLogTileRoot, ColorMetric.TOTAL_LOG, tileIndexes, renderCache);
                        logSnapshotStage(snapshotId, "render totalLog tiles done");
                    }
                    ColorScale walkColorScale = null;
                    if (renderModes.contains("walk")) {
                        logSnapshotStage(snapshotId, "render walk tiles start");
                        walkColorScale = renderTiles(segments, walkTileRoot, ColorMetric.WALK, tileIndexes, renderCache);
                        logSnapshotStage(snapshotId, "render walk tiles done");
                    }
                    ColorScale stopTransportColorScale = null;
                    if (renderModes.contains("stopTransport")) {
                        logSnapshotStage(snapshotId, "render stopTransport tiles start");
                        stopTransportColorScale = renderTiles(segments, stopTransportTileRoot, ColorMetric.STOP_TRANSPORT, tileIndexes, renderCache);
                        logSnapshotStage(snapshotId, "render stopTransport tiles done");
                    }
                    logSnapshotStage(snapshotId, "calculate contours start");
                    ContourResult contourResult = calculateContourResult(segments, renderCache, householdIndex);
                    logSnapshotStage(snapshotId, "calculate contours done");
                    snapshots.add(new SnapshotPayload(
                            snapshotId,
                            snapshotDate,
                            snapshotTime,
                            reachability,
                            segments.size(),
                            contourResult.stats,
                            contourResult.polygons,
                            totalColorScale,
                            totalNormalizedColorScale,
                            totalLogColorScale,
                            walkColorScale,
                            stopTransportColorScale,
                            tileUrlPrefix
                    ));
                    writeContourStats(spark, contourStatsDir, originSlug, originStopQuery, snapshots);
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
        String text = System.getenv().getOrDefault("BUS_ACCESSIBILITY_RENDER_MODES", "total,totalNormalized,totalLog,walk,stopTransport").trim();
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

    private static String normalizeTileUrlPrefix(String prefix) {
        String normalized = prefix == null ? "" : prefix.trim().replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isBlank() || normalized.contains("..")) {
            throw new IllegalArgumentException("Invalid BUS_ACCESSIBILITY_TILE_URL_PREFIX: " + prefix);
        }
        return normalized;
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
        Map<String, AccessLabel> accessByNode = calculateRoadGraphAccess(graph, stops, EFFECTIVE_WALK_RADIUS_METERS);
        Map<String, AccessLabel> nearestStopByNode = calculateRoadGraphNearestStopAccess(graph, stops, EFFECTIVE_WALK_RADIUS_METERS);
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
                    BestAccess best = bestAccessOnRoadSegment(a, b, mid, distance, aAccess, bAccess, EFFECTIVE_WALK_RADIUS_METERS);
                    if (best == null) {
                        continue;
                    }
                    BestAccess nearest = nearestAccessOnRoadSegment(a, b, mid, distance, aNearest, bNearest, EFFECTIVE_WALK_RADIUS_METERS);
                    if (nearest == null) {
                        nearest = best;
                    }
                    segments.add(new AccessibleSegment(
                            segments.size(),
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

    private static void buildAndWriteWalkCache(
            Path cacheFile,
            RoadGraph graph,
            List<RoadWay> roads,
            Map<String, StopPoint> stopsById
    ) throws Exception {
        long started = System.currentTimeMillis();
        Files.createDirectories(cacheFile.getParent());
        List<CachedSegmentBuilder> builders = buildCachedSegmentBuilders(graph, roads);
        Map<Integer, List<TemplateEndpoint>> endpointsByNode = new HashMap<>();
        for (int i = 0; i < builders.size(); i++) {
            CachedSegmentBuilder builder = builders.get(i);
            endpointsByNode.computeIfAbsent(builder.aNodeId, ignored -> new ArrayList<>())
                    .add(new TemplateEndpoint(i, builder.extraFromA));
            endpointsByNode.computeIfAbsent(builder.bNodeId, ignored -> new ArrayList<>())
                    .add(new TemplateEndpoint(i, builder.extraFromB));
        }
        List<StopPoint> stops = new ArrayList<>(stopsById.values());
        stops.sort(Comparator.comparing(stop -> stop.stopId));
        int index = 0;
        long candidateUpdates = 0L;
        for (StopPoint stop : stops) {
            Map<Integer, Double> walkByNode = calculateWalkMetersByNode(graph, stop, EFFECTIVE_WALK_RADIUS_METERS);
            for (Map.Entry<Integer, Double> entry : walkByNode.entrySet()) {
                List<TemplateEndpoint> endpoints = endpointsByNode.get(entry.getKey());
                if (endpoints == null) {
                    continue;
                }
                double nodeWalkMeters = entry.getValue();
                for (TemplateEndpoint endpoint : endpoints) {
                    double walkMeters = nodeWalkMeters + endpoint.extraMeters;
                    if (walkMeters <= EFFECTIVE_WALK_RADIUS_METERS) {
                        builders.get(endpoint.templateIndex).addCandidate(stop.stopId, walkMeters);
                        candidateUpdates++;
                    }
                }
            }
            index++;
            if (index % 100 == 0) {
                System.out.printf(
                        Locale.ROOT,
                        "BusAccessibilityMapCacheJob: walk-cache progress stops=%d/%d candidateUpdates=%d elapsedSec=%.1f%n",
                        index,
                        stops.size(),
                        candidateUpdates,
                        (System.currentTimeMillis() - started) / 1000.0
                );
            }
        }
        List<CachedSegmentTemplate> templates = builders.stream()
                .map(CachedSegmentBuilder::build)
                .filter(template -> !template.candidates.isEmpty())
                .collect(Collectors.toList());
        if (PRIMITIVE_WALK_CACHE) {
            writePrimitiveWalkCache(cacheFile, templates);
        } else {
            writeWalkCache(cacheFile, templates);
        }
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: wrote walk cache %s segments=%d/%d candidateUpdates=%d elapsedSec=%.1f%n",
                cacheFile,
                templates.size(),
                builders.size(),
                candidateUpdates,
                (System.currentTimeMillis() - started) / 1000.0
        );
    }

    private static List<CachedSegmentBuilder> buildCachedSegmentBuilders(RoadGraph graph, List<RoadWay> roads) {
        List<CachedSegmentBuilder> builders = new ArrayList<>();
        for (RoadWay road : roads) {
            for (int i = 1; i < road.points.size(); i++) {
                Point a = road.points.get(i - 1);
                Point b = road.points.get(i);
                int aNode = graph.nodeIdsByPointKey.getOrDefault(pointKey(a), -1);
                int bNode = graph.nodeIdsByPointKey.getOrDefault(pointKey(b), -1);
                if (aNode < 0 || bNode < 0) {
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
                    builders.add(new CachedSegmentBuilder(
                            p0,
                            p1,
                            aNode,
                            bNode,
                            haversineMeters(a.lat, a.lon, mid.lat, mid.lon),
                            Math.min(haversineMeters(b.lat, b.lon, mid.lat, mid.lon), distance),
                            haversineMeters(p0.lat, p0.lon, p1.lat, p1.lon)
                    ));
                }
            }
        }
        return builders;
    }

    private static Map<Integer, Double> calculateWalkMetersByNode(RoadGraph graph, StopPoint stop, double maxWalkMeters) {
        PriorityQueue<WalkState> queue = new PriorityQueue<>(Comparator.comparingDouble(state -> state.walkMeters));
        Map<Integer, Double> bestByNode = new HashMap<>();
        List<NodeDistance> connectors = graph.closestNodes(stop.lat, stop.lon, STOP_CONNECT_RADIUS_METERS, STOP_CONNECT_FALLBACK_NODES);
        for (NodeDistance connector : connectors) {
            if (connector.distanceMeters > maxWalkMeters) {
                continue;
            }
            Double previous = bestByNode.get(connector.nodeId);
            if (previous == null || connector.distanceMeters < previous) {
                bestByNode.put(connector.nodeId, connector.distanceMeters);
                queue.add(new WalkState(connector.nodeId, connector.distanceMeters));
            }
        }
        while (!queue.isEmpty()) {
            WalkState current = queue.poll();
            Double known = bestByNode.get(current.nodeId);
            if (known == null || current.walkMeters > known + 0.001) {
                continue;
            }
            for (RoadEdge edge : graph.edgesByNode.getOrDefault(current.nodeId, List.of())) {
                double nextWalkMeters = current.walkMeters + edge.distanceMeters;
                if (nextWalkMeters > maxWalkMeters) {
                    continue;
                }
                Double previous = bestByNode.get(edge.toNodeId);
                if (previous == null || nextWalkMeters < previous) {
                    bestByNode.put(edge.toNodeId, nextWalkMeters);
                    queue.add(new WalkState(edge.toNodeId, nextWalkMeters));
                }
            }
        }
        return bestByNode;
    }

    private static List<AccessibleSegment> buildAccessibilitySegments(WalkCache cache, List<StopTime> stops) {
        if (cache.primitive != null) {
            return buildAccessibilitySegments(cache.primitive, stops);
        }
        Map<String, StopTime> transportByStopId = stops.stream()
                .collect(Collectors.toMap(
                        stop -> stop.stopId,
                        stop -> stop,
                        (left, right) -> left.transportSeconds <= right.transportSeconds ? left : right,
                        HashMap::new
                ));
        if (PARALLEL_SEGMENT_BUILD) {
            return cache.segments.parallelStream()
                    .map(template -> buildAccessibleSegment(template, transportByStopId))
                    .filter(segment -> segment != null)
                    .collect(Collectors.toList());
        }
        List<AccessibleSegment> segments = new ArrayList<>(cache.segments.size());
        for (CachedSegmentTemplate template : cache.segments) {
            AccessibleSegment segment = buildAccessibleSegment(template, transportByStopId);
            if (segment != null) {
                segments.add(segment);
            }
        }
        return segments;
    }

    private static AccessibleSegment buildAccessibleSegment(
            CachedSegmentTemplate template,
            Map<String, StopTime> transportByStopId
    ) {
        BestAccess best = null;
        BestAccess nearest = null;
        for (WalkCandidate candidate : template.candidates) {
            StopTime stop = transportByStopId.get(candidate.stopId);
            if (stop == null) {
                continue;
            }
            double walkSeconds = candidate.walkMeters / WALK_SPEED_MPS;
            double totalSeconds = stop.transportSeconds + walkSeconds;
            if (best == null || totalSeconds < best.totalSeconds) {
                best = new BestAccess(stop, candidate.walkMeters, walkSeconds, totalSeconds);
            }
            if (nearest == null || candidate.walkMeters < nearest.walkDistanceMeters) {
                nearest = new BestAccess(stop, candidate.walkMeters, walkSeconds, totalSeconds);
            }
        }
        if (best == null) {
            return null;
        }
        if (nearest == null) {
            nearest = best;
        }
        return new AccessibleSegment(
                -1,
                template.from,
                template.to,
                best.totalSeconds / 60.0,
                best.walkSeconds / 60.0,
                best.walkDistanceMeters,
                best.stop.transportSeconds / 60.0,
                nearest.stop.transportSeconds / 60.0,
                nearest.walkDistanceMeters,
                template.lengthMeters,
                best.stop.stopId,
                best.stop.stopName
        );
    }

    private static List<AccessibleSegment> buildAccessibilitySegments(PrimitiveWalkCache cache, List<StopTime> stops) {
        double[] transportSecondsByStopIndex = new double[cache.stopIds.length];
        Arrays.fill(transportSecondsByStopIndex, Double.POSITIVE_INFINITY);
        StopTime[] stopByIndex = new StopTime[cache.stopIds.length];
        for (StopTime stop : stops) {
            Integer index = cache.stopIndexById.get(stop.stopId);
            if (index == null) {
                continue;
            }
            if (stop.transportSeconds < transportSecondsByStopIndex[index]) {
                transportSecondsByStopIndex[index] = stop.transportSeconds;
                stopByIndex[index] = stop;
            }
        }

        List<AccessibleSegment> segments = new ArrayList<>(cache.segmentCount);
        for (int segmentIndex = 0; segmentIndex < cache.segmentCount; segmentIndex++) {
            int candidateStart = cache.candidateOffsets[segmentIndex];
            int candidateEnd = cache.candidateOffsets[segmentIndex + 1];
            int bestStopIndex = -1;
            int nearestStopIndex = -1;
            double bestWalkMeters = 0.0;
            double nearestWalkMeters = 0.0;
            double bestWalkSeconds = 0.0;
            double nearestWalkSeconds = 0.0;
            double bestTotalSeconds = Double.POSITIVE_INFINITY;
            double nearestDistanceMeters = Double.POSITIVE_INFINITY;

            for (int candidateIndex = candidateStart; candidateIndex < candidateEnd; candidateIndex++) {
                int stopIndex = cache.candidateStopIndexes[candidateIndex];
                double transportSeconds = transportSecondsByStopIndex[stopIndex];
                if (!Double.isFinite(transportSeconds)) {
                    continue;
                }
                double walkMeters = cache.candidateWalkMeters[candidateIndex];
                double walkSeconds = walkMeters / WALK_SPEED_MPS;
                double totalSeconds = transportSeconds + walkSeconds;
                if (totalSeconds < bestTotalSeconds) {
                    bestTotalSeconds = totalSeconds;
                    bestStopIndex = stopIndex;
                    bestWalkMeters = walkMeters;
                    bestWalkSeconds = walkSeconds;
                }
                if (walkMeters < nearestDistanceMeters) {
                    nearestDistanceMeters = walkMeters;
                    nearestStopIndex = stopIndex;
                    nearestWalkMeters = walkMeters;
                    nearestWalkSeconds = walkSeconds;
                }
            }
            if (bestStopIndex < 0) {
                continue;
            }
            if (nearestStopIndex < 0) {
                nearestStopIndex = bestStopIndex;
                nearestWalkMeters = bestWalkMeters;
                nearestWalkSeconds = bestWalkSeconds;
            }
            StopTime bestStop = stopByIndex[bestStopIndex];
            StopTime nearestStop = stopByIndex[nearestStopIndex];
            segments.add(new AccessibleSegment(
                    segmentIndex,
                    new Point(cache.fromLat[segmentIndex], cache.fromLon[segmentIndex]),
                    new Point(cache.toLat[segmentIndex], cache.toLon[segmentIndex]),
                    bestTotalSeconds / 60.0,
                    bestWalkSeconds / 60.0,
                    bestWalkMeters,
                    bestStop.transportSeconds / 60.0,
                    nearestStop.transportSeconds / 60.0,
                    nearestWalkMeters,
                    cache.lengthMeters[segmentIndex],
                    bestStop.stopId,
                    bestStop.stopName
            ));
        }
        return segments;
    }

    private static void writeWalkCache(Path cacheFile, List<CachedSegmentTemplate> templates) throws Exception {
        Path tempFile = Files.createTempFile(cacheFile.getParent(), cacheFile.getFileName().toString(), ".tmp");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(tempFile))))) {
            output.writeInt(WALK_CACHE_MAGIC);
            output.writeInt(WALK_CACHE_VERSION);
            output.writeDouble(WALK_SPEED_MPS);
            output.writeDouble(EFFECTIVE_WALK_RADIUS_METERS);
            output.writeDouble(SAMPLE_METERS);
            output.writeInt(templates.size());
            for (CachedSegmentTemplate template : templates) {
                output.writeDouble(template.from.lat);
                output.writeDouble(template.from.lon);
                output.writeDouble(template.to.lat);
                output.writeDouble(template.to.lon);
                output.writeDouble(template.lengthMeters);
                output.writeInt(template.candidates.size());
                for (WalkCandidate candidate : template.candidates) {
                    output.writeUTF(candidate.stopId);
                    output.writeFloat((float) candidate.walkMeters);
                }
            }
        }
        Files.move(tempFile, cacheFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void writePrimitiveWalkCache(Path cacheFile, List<CachedSegmentTemplate> templates) throws Exception {
        Path tempFile = Files.createTempFile(cacheFile.getParent(), cacheFile.getFileName().toString(), ".tmp");
        Map<String, Integer> stopIndexById = new LinkedHashMap<>();
        int candidateCount = 0;
        for (CachedSegmentTemplate template : templates) {
            candidateCount += template.candidates.size();
            for (WalkCandidate candidate : template.candidates) {
                stopIndexById.computeIfAbsent(candidate.stopId, ignored -> stopIndexById.size());
            }
        }
        String[] stopIds = new String[stopIndexById.size()];
        for (Map.Entry<String, Integer> entry : stopIndexById.entrySet()) {
            stopIds[entry.getValue()] = entry.getKey();
        }
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(tempFile))))) {
            output.writeInt(PRIMITIVE_WALK_CACHE_MAGIC);
            output.writeInt(PRIMITIVE_WALK_CACHE_VERSION);
            output.writeDouble(WALK_SPEED_MPS);
            output.writeDouble(EFFECTIVE_WALK_RADIUS_METERS);
            output.writeDouble(SAMPLE_METERS);
            output.writeInt(stopIds.length);
            for (String stopId : stopIds) {
                output.writeUTF(stopId);
            }
            output.writeInt(templates.size());
            output.writeInt(candidateCount);
            int offset = 0;
            for (CachedSegmentTemplate template : templates) {
                output.writeDouble(template.from.lat);
                output.writeDouble(template.from.lon);
                output.writeDouble(template.to.lat);
                output.writeDouble(template.to.lon);
                output.writeFloat((float) template.lengthMeters);
                output.writeInt(offset);
                output.writeInt(template.candidates.size());
                offset += template.candidates.size();
            }
            for (CachedSegmentTemplate template : templates) {
                for (WalkCandidate candidate : template.candidates) {
                    output.writeInt(stopIndexById.get(candidate.stopId));
                    output.writeFloat((float) candidate.walkMeters);
                }
            }
        }
        Files.move(tempFile, cacheFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void buildAndWriteRenderCache(Path cacheFile, PrimitiveWalkCache walkCache) throws Exception {
        long started = System.currentTimeMillis();
        Files.createDirectories(cacheFile.getParent());
        RenderCache cache = buildRenderCache(walkCache);
        Path tempFile = Files.createTempFile(cacheFile.getParent(), cacheFile.getFileName().toString(), ".tmp");
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(tempFile))))) {
            output.writeInt(RENDER_CACHE_MAGIC);
            output.writeInt(RENDER_CACHE_VERSION);
            output.writeInt(MIN_ZOOM);
            output.writeInt(OVERLAY_MAX_ZOOM);
            output.writeInt(cache.segmentCount);
            output.writeInt(cache.tiles.size());
            for (CachedTile tile : cache.tiles) {
                output.writeUTF(tile.key);
                output.writeInt(tile.zoom);
                output.writeInt(tile.commandSegmentIndexes.length);
                for (int i = 0; i < tile.commandSegmentIndexes.length; i++) {
                    output.writeInt(tile.commandSegmentIndexes[i]);
                    output.writeShort(tile.x0[i]);
                    output.writeShort(tile.y0[i]);
                    output.writeShort(tile.x1[i]);
                    output.writeShort(tile.y1[i]);
                    output.writeShort(tile.width[i]);
                }
            }
            output.writeInt(cache.contourCellOffsets.length);
            for (int offset : cache.contourCellOffsets) {
                output.writeInt(offset);
            }
            output.writeInt(cache.contourCellKeys.length);
            for (long key : cache.contourCellKeys) {
                output.writeLong(key);
            }
        }
        Files.move(tempFile, cacheFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: wrote render cache %s segments=%d tiles=%d commands=%d contourCells=%d elapsedSec=%.1f%n",
                cacheFile,
                cache.segmentCount,
                cache.tiles.size(),
                cache.commandCount,
                cache.contourCellKeys.length,
                (System.currentTimeMillis() - started) / 1000.0
        );
    }

    private static RenderCache buildRenderCache(PrimitiveWalkCache walkCache) {
        Map<String, CachedTileBuilder> tileBuilders = new HashMap<>();
        List<long[]> contourCellsBySegment = new ArrayList<>(walkCache.segmentCount);
        int commandCount = 0;
        int contourCellCount = 0;
        double cellMeters = Math.max(25.0, SAMPLE_METERS);
        int radiusCells = Math.max(0, (int) Math.ceil(CONTOUR_ENVELOPE_METERS / cellMeters));
        for (int segmentIndex = 0; segmentIndex < walkCache.segmentCount; segmentIndex++) {
            Point from = new Point(walkCache.fromLat[segmentIndex], walkCache.fromLon[segmentIndex]);
            Point to = new Point(walkCache.toLat[segmentIndex], walkCache.toLon[segmentIndex]);
            for (int zoom = MIN_ZOOM; zoom <= OVERLAY_MAX_ZOOM; zoom++) {
                commandCount += addRenderCacheTileCommands(tileBuilders, segmentIndex, from, to, zoom);
            }
            long[] contourCells = renderCacheContourCells(from, to, walkCache.lengthMeters[segmentIndex], cellMeters, radiusCells);
            contourCellsBySegment.add(contourCells);
            contourCellCount += contourCells.length;
        }
        List<CachedTile> tiles = tileBuilders.values().stream()
                .map(CachedTileBuilder::build)
                .sorted(Comparator.comparing((CachedTile tile) -> tile.zoom).thenComparing(tile -> tile.key))
                .collect(Collectors.toList());
        int[] contourCellOffsets = new int[walkCache.segmentCount + 1];
        long[] contourCellKeys = new long[contourCellCount];
        int offset = 0;
        for (int i = 0; i < contourCellsBySegment.size(); i++) {
            contourCellOffsets[i] = offset;
            long[] cells = contourCellsBySegment.get(i);
            System.arraycopy(cells, 0, contourCellKeys, offset, cells.length);
            offset += cells.length;
        }
        contourCellOffsets[walkCache.segmentCount] = offset;
        return new RenderCache(walkCache.segmentCount, tiles, commandCount, contourCellOffsets, contourCellKeys);
    }

    private static int addRenderCacheTileCommands(
            Map<String, CachedTileBuilder> tileBuilders,
            int segmentIndex,
            Point from,
            Point to,
            int zoom
    ) {
        double ax = lonToPixelX(from.lon, zoom);
        double ay = latToPixelY(from.lat, zoom);
        double bx = lonToPixelX(to.lon, zoom);
        double by = latToPixelY(to.lat, zoom);
        int margin = Math.max(5, strokeWidth(zoom) + 2);
        int minTileX = (int) Math.floor((Math.min(ax, bx) - margin) / TILE_SIZE);
        int maxTileX = (int) Math.floor((Math.max(ax, bx) + margin) / TILE_SIZE);
        int minTileY = (int) Math.floor((Math.min(ay, by) - margin) / TILE_SIZE);
        int maxTileY = (int) Math.floor((Math.max(ay, by) + margin) / TILE_SIZE);
        int count = 0;
        int width = strokeWidth(zoom);
        for (int tileX = minTileX; tileX <= maxTileX; tileX++) {
            for (int tileY = minTileY; tileY <= maxTileY; tileY++) {
                int offsetX = tileX * TILE_SIZE;
                int offsetY = tileY * TILE_SIZE;
                String key = zoom + "/" + tileX + "/" + tileY;
                tileBuilders.computeIfAbsent(key, ignored -> new CachedTileBuilder(key, zoom))
                        .add(
                                segmentIndex,
                                clampShort(Math.round(ax - offsetX)),
                                clampShort(Math.round(ay - offsetY)),
                                clampShort(Math.round(bx - offsetX)),
                                clampShort(Math.round(by - offsetY)),
                                width
                        );
                count++;
            }
        }
        return count;
    }

    private static short clampShort(long value) {
        return (short) Math.max(Short.MIN_VALUE, Math.min(Short.MAX_VALUE, value));
    }

    private static long[] renderCacheContourCells(Point from, Point to, double lengthMeters, double cellMeters, int radiusCells) {
        Set<Long> occupiedCells = new HashSet<>();
        int steps = Math.max(1, (int) Math.ceil(lengthMeters / cellMeters));
        for (int step = 0; step <= steps; step++) {
            double t = step / (double) steps;
            Point point = interpolate(from, to, t);
            addContourAreaCellKeys(occupiedCells, point, cellMeters, radiusCells);
        }
        long[] cells = new long[occupiedCells.size()];
        int index = 0;
        for (Long cell : occupiedCells) {
            cells[index++] = cell;
        }
        Arrays.sort(cells);
        return cells;
    }

    private static RenderCache readRenderCache(Path cacheFile) throws Exception {
        long started = System.currentTimeMillis();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(new GZIPInputStream(Files.newInputStream(cacheFile))))) {
            int magic = input.readInt();
            int version = input.readInt();
            if (magic != RENDER_CACHE_MAGIC || version != RENDER_CACHE_VERSION) {
                throw new IllegalStateException("Unsupported accessibility render cache: " + cacheFile);
            }
            int minZoom = input.readInt();
            int overlayMaxZoom = input.readInt();
            int segmentCount = input.readInt();
            int tileCount = input.readInt();
            List<CachedTile> tiles = new ArrayList<>(tileCount);
            int commandCount = 0;
            for (int i = 0; i < tileCount; i++) {
                String key = input.readUTF();
                int zoom = input.readInt();
                int count = input.readInt();
                int[] segmentIndexes = new int[count];
                short[] x0 = new short[count];
                short[] y0 = new short[count];
                short[] x1 = new short[count];
                short[] y1 = new short[count];
                short[] width = new short[count];
                for (int j = 0; j < count; j++) {
                    segmentIndexes[j] = input.readInt();
                    x0[j] = input.readShort();
                    y0[j] = input.readShort();
                    x1[j] = input.readShort();
                    y1[j] = input.readShort();
                    width[j] = input.readShort();
                }
                tiles.add(new CachedTile(key, zoom, segmentIndexes, x0, y0, x1, y1, width));
                commandCount += count;
            }
            int offsetCount = input.readInt();
            int[] contourCellOffsets = new int[offsetCount];
            for (int i = 0; i < offsetCount; i++) {
                contourCellOffsets[i] = input.readInt();
            }
            int cellCount = input.readInt();
            long[] contourCellKeys = new long[cellCount];
            for (int i = 0; i < cellCount; i++) {
                contourCellKeys[i] = input.readLong();
            }
            if (minZoom != MIN_ZOOM || overlayMaxZoom < OVERLAY_MAX_ZOOM) {
                System.out.printf(
                        Locale.ROOT,
                        "BusAccessibilityMapCacheJob: render cache zoom mismatch cache=%d-%d runtime=%d-%d; missing zooms use fallback%n",
                        minZoom,
                        overlayMaxZoom,
                        MIN_ZOOM,
                        OVERLAY_MAX_ZOOM
                );
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusAccessibilityMapCacheJob: loaded render cache %s segments=%d tiles=%d commands=%d contourCells=%d elapsedSec=%.1f%n",
                    cacheFile,
                    segmentCount,
                    tiles.size(),
                    commandCount,
                    contourCellKeys.length,
                    (System.currentTimeMillis() - started) / 1000.0
            );
            return new RenderCache(segmentCount, tiles, commandCount, contourCellOffsets, contourCellKeys);
        }
    }

    private static HouseholdIndex loadHouseholdIndex(Path householdsFile) {
        if (householdsFile == null || !Files.isRegularFile(householdsFile)) {
            System.out.printf(
                    Locale.ROOT,
                    "BusAccessibilityMapCacheJob: household file is not available, household contour stats disabled: %s%n",
                    householdsFile
            );
            return HouseholdIndex.empty();
        }
        long started = System.currentTimeMillis();
        try {
            String json = Files.readString(householdsFile);
            if (!json.isEmpty() && json.charAt(0) == '\ufeff') {
                json = json.substring(1);
            }
            JSONArray array = new JSONArray(json);
            Map<Long, Double> householdsByCell = new HashMap<>();
            int accepted = 0;
            double totalHouseholds = 0.0;
            double cellMeters = Math.max(25.0, SAMPLE_METERS);
            for (int i = 0; i < array.length(); i++) {
                JSONObject object = array.optJSONObject(i);
                if (object == null) {
                    continue;
                }
                double lat = object.optDouble("lat", Double.NaN);
                double lon = object.optDouble("lon", Double.NaN);
                if (!Double.isFinite(lat) || !Double.isFinite(lon)) {
                    continue;
                }
                double flats = object.optDouble("flats", 0.0);
                double households = Math.max(1.0, flats);
                Point point = new Point(lat, lon);
                long key = contourCellKey(contourAreaCellX(point, cellMeters), contourAreaCellY(point, cellMeters));
                householdsByCell.merge(key, households, Double::sum);
                totalHouseholds += households;
                accepted++;
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusAccessibilityMapCacheJob: loaded households %s rows=%d cells=%d households=%.1f elapsedSec=%.1f%n",
                    householdsFile,
                    accepted,
                    householdsByCell.size(),
                    totalHouseholds,
                    (System.currentTimeMillis() - started) / 1000.0
            );
            return new HouseholdIndex(householdsByCell, accepted, totalHouseholds);
        } catch (Exception e) {
            System.err.printf(
                    Locale.ROOT,
                    "BusAccessibilityMapCacheJob: failed to load households from %s: %s%n",
                    householdsFile,
                    e
            );
            return HouseholdIndex.empty();
        }
    }

    private static WalkCache readWalkCache(Path cacheFile) throws Exception {
        long started = System.currentTimeMillis();
        List<CachedSegmentTemplate> templates = new ArrayList<>();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(new GZIPInputStream(Files.newInputStream(cacheFile))))) {
            int magic = input.readInt();
            int version = input.readInt();
            if (magic == PRIMITIVE_WALK_CACHE_MAGIC) {
                if (version != PRIMITIVE_WALK_CACHE_VERSION) {
                    throw new IllegalStateException("Unsupported primitive accessibility walk cache: " + cacheFile);
                }
                PrimitiveWalkCache primitive = readPrimitiveWalkCacheBody(input);
                System.out.printf(
                        Locale.ROOT,
                        "BusAccessibilityMapCacheJob: loaded primitive walk cache %s segments=%d candidates=%d stops=%d elapsedSec=%.1f%n",
                        cacheFile,
                        primitive.segmentCount,
                        primitive.candidateStopIndexes.length,
                        primitive.stopIds.length,
                        (System.currentTimeMillis() - started) / 1000.0
                );
                return new WalkCache(null, primitive);
            }
            if (magic != WALK_CACHE_MAGIC || version != WALK_CACHE_VERSION) {
                throw new IllegalStateException("Unsupported accessibility walk cache: " + cacheFile);
            }
            input.readDouble(); // walk speed used for build; current runtime speed remains authoritative.
            input.readDouble();
            input.readDouble();
            int segmentCount = input.readInt();
            for (int i = 0; i < segmentCount; i++) {
                Point from = new Point(input.readDouble(), input.readDouble());
                Point to = new Point(input.readDouble(), input.readDouble());
                double lengthMeters = input.readDouble();
                int candidateCount = input.readInt();
                List<WalkCandidate> candidates = new ArrayList<>(candidateCount);
                for (int j = 0; j < candidateCount; j++) {
                    candidates.add(new WalkCandidate(input.readUTF(), input.readFloat()));
                }
                templates.add(new CachedSegmentTemplate(from, to, lengthMeters, candidates));
            }
        }
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: loaded walk cache %s segments=%d elapsedSec=%.1f%n",
                cacheFile,
                templates.size(),
                (System.currentTimeMillis() - started) / 1000.0
        );
        return new WalkCache(templates, null);
    }

    private static PrimitiveWalkCache readPrimitiveWalkCacheBody(DataInputStream input) throws Exception {
        input.readDouble(); // walk speed used for build; current runtime speed remains authoritative.
        input.readDouble();
        input.readDouble();
        int stopCount = input.readInt();
        String[] stopIds = new String[stopCount];
        Map<String, Integer> stopIndexById = new HashMap<>(stopCount * 2);
        for (int i = 0; i < stopCount; i++) {
            String stopId = input.readUTF();
            stopIds[i] = stopId;
            stopIndexById.put(stopId, i);
        }
        int segmentCount = input.readInt();
        int candidateCount = input.readInt();
        double[] fromLat = new double[segmentCount];
        double[] fromLon = new double[segmentCount];
        double[] toLat = new double[segmentCount];
        double[] toLon = new double[segmentCount];
        float[] lengthMeters = new float[segmentCount];
        int[] candidateOffsets = new int[segmentCount + 1];
        for (int i = 0; i < segmentCount; i++) {
            fromLat[i] = input.readDouble();
            fromLon[i] = input.readDouble();
            toLat[i] = input.readDouble();
            toLon[i] = input.readDouble();
            lengthMeters[i] = input.readFloat();
            int offset = input.readInt();
            int count = input.readInt();
            candidateOffsets[i] = offset;
            candidateOffsets[i + 1] = offset + count;
        }
        int[] candidateStopIndexes = new int[candidateCount];
        float[] candidateWalkMeters = new float[candidateCount];
        for (int i = 0; i < candidateCount; i++) {
            candidateStopIndexes[i] = input.readInt();
            candidateWalkMeters[i] = input.readFloat();
        }
        return new PrimitiveWalkCache(
                stopIds,
                stopIndexById,
                fromLat,
                fromLon,
                toLat,
                toLon,
                lengthMeters,
                candidateOffsets,
                candidateStopIndexes,
                candidateWalkMeters
        );
    }

    private static Map<String, AccessLabel> calculateRoadGraphAccess(RoadGraph graph, List<StopTime> stops, double maxWalkMeters) {
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
                if (nextWalkMeters > maxWalkMeters) {
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

    private static Map<String, AccessLabel> calculateRoadGraphNearestStopAccess(RoadGraph graph, List<StopTime> stops, double maxWalkMeters) {
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
                if (nextWalkMeters > maxWalkMeters) {
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
            AccessLabel bAccess,
            double maxWalkMeters
    ) {
        BestAccess best = null;
        if (aAccess != null) {
            double distanceFromA = haversineMeters(a.lat, a.lon, point.lat, point.lon);
            best = candidateAccess(best, aAccess, distanceFromA, maxWalkMeters);
        }
        if (bAccess != null) {
            double distanceFromB = haversineMeters(b.lat, b.lon, point.lat, point.lon);
            best = candidateAccess(best, bAccess, Math.min(distanceFromB, segmentDistance), maxWalkMeters);
        }
        return best;
    }

    private static BestAccess candidateAccess(BestAccess best, AccessLabel access, double extraMeters, double maxWalkMeters) {
        double walkMeters = access.walkMeters + extraMeters;
        if (walkMeters > maxWalkMeters) {
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
            AccessLabel bAccess,
            double maxWalkMeters
    ) {
        BestAccess best = null;
        if (aAccess != null) {
            double distanceFromA = haversineMeters(a.lat, a.lon, point.lat, point.lon);
            best = candidateNearestAccess(best, aAccess, distanceFromA, maxWalkMeters);
        }
        if (bAccess != null) {
            double distanceFromB = haversineMeters(b.lat, b.lon, point.lat, point.lon);
            best = candidateNearestAccess(best, bAccess, Math.min(distanceFromB, segmentDistance), maxWalkMeters);
        }
        return best;
    }

    private static BestAccess candidateNearestAccess(BestAccess best, AccessLabel access, double extraMeters, double maxWalkMeters) {
        double walkMeters = access.walkMeters + extraMeters;
        if (walkMeters > maxWalkMeters) {
            return best;
        }
        double walkSeconds = walkMeters / WALK_SPEED_MPS;
        double totalSeconds = access.stop.transportSeconds + walkSeconds;
        if (best == null || walkMeters < best.walkDistanceMeters) {
            return new BestAccess(access.stop, walkMeters, walkSeconds, totalSeconds);
        }
        return best;
    }

    private static void debugTopTotalSegments(LocalDate serviceDate, LocalTime departureTime, List<AccessibleSegment> segments) {
        if (!Boolean.parseBoolean(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEBUG_TOP_TOTAL", "false"))) {
            return;
        }
        String onlyTime = System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEBUG_DEPARTURE_TIME", "").trim();
        if (!onlyTime.isEmpty() && !onlyTime.equals(departureTime.toString())) {
            return;
        }
        double threshold = Double.parseDouble(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEBUG_MIN_TOTAL_MINUTES", "700"));
        int limit = Integer.parseInt(System.getenv().getOrDefault("BUS_ACCESSIBILITY_DEBUG_LIMIT", "20"));
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: debug top total segments serviceDate=%s departureTime=%s threshold=%.1f limit=%d%n",
                serviceDate,
                departureTime,
                threshold,
                limit
        );
        segments.stream()
                .filter(segment -> segment.totalMinutes >= threshold)
                .sorted(Comparator.comparingDouble((AccessibleSegment segment) -> segment.totalMinutes).reversed())
                .limit(limit)
                .forEach(segment -> {
                    double lat = (segment.from.lat + segment.to.lat) / 2.0;
                    double lon = (segment.from.lon + segment.to.lon) / 2.0;
                    System.out.printf(
                            Locale.ROOT,
                            "ACCESS_DEBUG_TOP serviceDate=%s departure=%s totalMin=%.1f transportMin=%.1f walkMin=%.1f walkMeters=%.0f stopId=%s stopName=%s lat=%.6f lon=%.6f%n",
                            serviceDate,
                            departureTime,
                            segment.totalMinutes,
                            segment.stopTransportMinutes,
                            segment.walkMinutes,
                            segment.walkDistanceMeters,
                            segment.nearestStopId,
                            segment.nearestStopName,
                            lat,
                            lon
                    );
                });
    }

    private static void logSnapshotStage(String snapshotId, String stage) {
        Runtime runtime = Runtime.getRuntime();
        long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
        long totalMb = runtime.totalMemory() / 1024 / 1024;
        long maxMb = runtime.maxMemory() / 1024 / 1024;
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityMapCacheJob: stage snapshot=%s stage=\"%s\" heapUsedMb=%d heapTotalMb=%d heapMaxMb=%d at=%s%n",
                snapshotId,
                stage,
                usedMb,
                totalMb,
                maxMb,
                Instant.now()
        );
    }

    private static String pointKey(Point point) {
        return String.format(Locale.ROOT, "%.7f|%.7f", point.lat, point.lon);
    }

    private static ColorScale renderTiles(
            List<AccessibleSegment> segments,
            Path tileRoot,
            ColorMetric metric,
            Map<Integer, Map<String, List<AccessibleSegment>>> tileIndexes,
            RenderCache renderCache
    ) throws Exception {
        ColorScale colorScale = ColorScale.fromSegments(segments, metric);
        Path tempRoot = tileRoot.resolveSibling(tileRoot.getFileName() + ".tmp-" + System.currentTimeMillis());
        Files.createDirectories(tempRoot);
        try {
            if (renderCache != null && renderCache.segmentCount > 0 && segments.stream().allMatch(segment -> segment.segmentIndex >= 0)) {
                double[] valuesBySegmentIndex = metricValuesBySegmentIndex(segments, metric, renderCache.segmentCount);
                renderCache.tiles.stream()
                        .filter(tile -> tile.zoom >= MIN_ZOOM && tile.zoom <= OVERLAY_MAX_ZOOM)
                        .parallel()
                        .forEach(tile -> writeTileUnchecked(
                                tempRoot.resolve(tile.key + ".png"),
                                renderTileCommands(tile, valuesBySegmentIndex, colorScale)
                        ));
            } else {
                for (int zoom = MIN_ZOOM; zoom <= OVERLAY_MAX_ZOOM; zoom++) {
                    Map<String, List<AccessibleSegment>> segmentsByTile = tileIndexes.getOrDefault(zoom, Map.of());
                    segmentsByTile.entrySet().parallelStream().forEach(entry -> writeTileUnchecked(
                            tempRoot.resolve(entry.getKey() + ".png"),
                            renderTileSegments(entry.getKey(), entry.getValue(), colorScale)
                    ));
                }
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
        } catch (Exception e) {
            cleanupTempRootQuietly(tempRoot);
            throw e;
        }
    }

    private static double[] metricValuesBySegmentIndex(List<AccessibleSegment> segments, ColorMetric metric, int segmentCount) {
        double[] values = new double[segmentCount];
        Arrays.fill(values, Double.NaN);
        for (AccessibleSegment segment : segments) {
            if (segment.segmentIndex >= 0 && segment.segmentIndex < values.length) {
                values[segment.segmentIndex] = metric.value(segment);
            }
        }
        return values;
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
        } catch (Exception e) {
            cleanupTempRootQuietly(tempRoot);
            throw e;
        }
    }

    private static Map<Integer, Map<String, List<AccessibleSegment>>> buildTileIndexes(
            List<AccessibleSegment> segments,
            double maxTotalMinutes
    ) {
        Map<Integer, Map<String, List<AccessibleSegment>>> indexes = new HashMap<>();
        for (int zoom = MIN_ZOOM; zoom <= OVERLAY_MAX_ZOOM; zoom++) {
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
            param.setCompressionQuality(Math.max(0.0f, Math.min(1.0f, PNG_COMPRESSION_QUALITY)));
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
        BufferedImage image = createOverlayImage();
        int width = strokeWidth(zoom);
        for (AccessibleSegment segment : segments) {
            int ax = (int) Math.round(lonToPixelX(segment.from.lon, zoom) - offsetX);
            int ay = (int) Math.round(latToPixelY(segment.from.lat, zoom) - offsetY);
            int bx = (int) Math.round(lonToPixelX(segment.to.lon, zoom) - offsetX);
            int by = (int) Math.round(latToPixelY(segment.to.lat, zoom) - offsetY);
            drawRasterLine(image, ax, ay, bx, by, width, accessibilityColor(colorScale.metric.value(segment), colorScale).getRGB());
        }
        return image;
    }

    private static BufferedImage renderTileCommands(CachedTile tile, double[] valuesBySegmentIndex, ColorScale colorScale) {
        BufferedImage image = createOverlayImage();
        for (int i = 0; i < tile.commandSegmentIndexes.length; i++) {
            int segmentIndex = tile.commandSegmentIndexes[i];
            if (segmentIndex < 0 || segmentIndex >= valuesBySegmentIndex.length) {
                continue;
            }
            double value = valuesBySegmentIndex[segmentIndex];
            if (!Double.isFinite(value)) {
                continue;
            }
            drawRasterLine(
                    image,
                    tile.x0[i],
                    tile.y0[i],
                    tile.x1[i],
                    tile.y1[i],
                    tile.width[i],
                    accessibilityColor(value, colorScale).getRGB()
            );
        }
        return image;
    }

    private static BufferedImage createOverlayImage() {
        if (!INDEXED_PNG) {
            return new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
        }
        byte[] red = new byte[256];
        byte[] green = new byte[256];
        byte[] blue = new byte[256];
        byte[] alpha = new byte[256];
        alpha[0] = 0;
        for (int i = 1; i < 256; i++) {
            Color color = rampColor((i - 1) / 254.0);
            red[i] = (byte) color.getRed();
            green[i] = (byte) color.getGreen();
            blue[i] = (byte) color.getBlue();
            alpha[i] = (byte) 210;
        }
        IndexColorModel colorModel = new IndexColorModel(8, 256, red, green, blue, alpha);
        return new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_BYTE_INDEXED, colorModel);
    }

    private static void drawRasterLine(BufferedImage image, int x0, int y0, int x1, int y1, int width, int rgb) {
        int dx = x1 - x0;
        int dy = y1 - y0;
        int steps = Math.max(Math.abs(dx), Math.abs(dy));
        if (steps == 0) {
            paintContourPixel(image, x0, y0, width, rgb);
            return;
        }
        for (int step = 0; step <= steps; step++) {
            double t = step / (double) steps;
            int x = (int) Math.round(x0 + dx * t);
            int y = (int) Math.round(y0 + dy * t);
            paintContourPixel(image, x, y, width, rgb);
        }
    }

    private static BufferedImage renderTileContours(String key, List<AccessibleSegment> segments) {
        String[] parts = key.split("/");
        int zoom = Integer.parseInt(parts[0]);
        int tileX = Integer.parseInt(parts[1]);
        int tileY = Integer.parseInt(parts[2]);
        int offsetX = tileX * TILE_SIZE;
        int offsetY = tileY * TILE_SIZE;
        BufferedImage image = new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
        drawContourBoundary(image, segments, zoom, offsetX, offsetY, 60.0, new Color(79, 166, 222, 215));
        drawContourBoundary(image, segments, zoom, offsetX, offsetY, 30.0, new Color(25, 105, 185, 225));
        drawContourBoundary(image, segments, zoom, offsetX, offsetY, 15.0, new Color(5, 51, 140, 235));
        return image;
    }

    private static void drawContourBoundary(
            BufferedImage target,
            List<AccessibleSegment> segments,
            int zoom,
            int offsetX,
            int offsetY,
            double thresholdMinutes,
            Color color
    ) {
        double tileCenterLat = pixelYToLat(offsetY + TILE_SIZE / 2.0, zoom);
        int streetWidth = Math.max(strokeWidth(zoom), metersToPixels(CONTOUR_ENVELOPE_METERS, tileCenterLat, zoom));
        int contourWidth = contourStrokeWidth(zoom);
        int pad = Math.max(streetWidth + contourWidth + 2, 8);
        BufferedImage mask = new BufferedImage(TILE_SIZE + pad * 2, TILE_SIZE + pad * 2, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D g = mask.createGraphics();
        try {
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);
            g.setStroke(new BasicStroke(streetWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
            g.setColor(Color.WHITE);
            for (AccessibleSegment segment : segments) {
                if (segment.totalMinutes > thresholdMinutes) {
                    continue;
                }
                double ax = lonToPixelX(segment.from.lon, zoom);
                double ay = latToPixelY(segment.from.lat, zoom);
                double bx = lonToPixelX(segment.to.lon, zoom);
                double by = latToPixelY(segment.to.lat, zoom);
                g.drawLine(
                        (int) Math.round(ax - offsetX + pad),
                        (int) Math.round(ay - offsetY + pad),
                        (int) Math.round(bx - offsetX + pad),
                        (int) Math.round(by - offsetY + pad)
                );
            }
        } finally {
            g.dispose();
        }
        int rgb = color.getRGB();
        for (int y = 0; y < TILE_SIZE; y++) {
            for (int x = 0; x < TILE_SIZE; x++) {
                int mx = x + pad;
                int my = y + pad;
                if (!isMaskSet(mask, mx, my) || !isMaskBoundary(mask, mx, my)) {
                    continue;
                }
                paintContourPixel(target, x, y, contourWidth, rgb);
            }
        }
    }

    private static boolean isMaskBoundary(BufferedImage mask, int x, int y) {
        for (int dy = -1; dy <= 1; dy++) {
            for (int dx = -1; dx <= 1; dx++) {
                if (dx == 0 && dy == 0) {
                    continue;
                }
                if (!isMaskSet(mask, x + dx, y + dy)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isMaskSet(BufferedImage mask, int x, int y) {
        if (x < 0 || y < 0 || x >= mask.getWidth() || y >= mask.getHeight()) {
            return false;
        }
        return (mask.getRGB(x, y) & 0xff) > 0;
    }

    private static void paintContourPixel(BufferedImage target, int x, int y, int width, int rgb) {
        int radius = Math.max(0, width / 2);
        for (int dy = -radius; dy <= radius; dy++) {
            int py = y + dy;
            if (py < 0 || py >= TILE_SIZE) {
                continue;
            }
            for (int dx = -radius; dx <= radius; dx++) {
                int px = x + dx;
                if (px >= 0 && px < TILE_SIZE) {
                    target.setRGB(px, py, rgb);
                }
            }
        }
    }

    private static int contourStrokeWidth(int zoom) {
        return Math.max(1, strokeWidth(zoom) / 2);
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
        Color color = rampColor(n);
        return new Color(color.getRed(), color.getGreen(), color.getBlue(), 210);
    }

    private static Color rampColor(double n) {
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
                (int) Math.round(a[2] + (b[2] - a[2]) * t)
        );
    }

    private static void clearDirectory(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        List<Path> paths;
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            paths = stream.sorted(Comparator
                    .comparingInt(Path::getNameCount)
                    .reversed()
                    .thenComparing(Comparator.reverseOrder()))
                    .collect(Collectors.toList());
        }
        for (Path path : paths) {
            if (!path.equals(dir)) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void cleanupTempRootQuietly(Path tempRoot) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                if (!Files.exists(tempRoot)) {
                    return;
                }
                clearDirectory(tempRoot);
                Files.deleteIfExists(tempRoot);
                return;
            } catch (Exception e) {
                System.err.printf(
                        Locale.ROOT,
                        "BusAccessibilityMapCacheJob: failed to cleanup temp tile root attempt=%d path=%s error=%s%n",
                        attempt,
                        tempRoot,
                        e
                );
                try {
                    Thread.sleep(250L * attempt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static ContourStats calculateContourStats(List<AccessibleSegment> segments) {
        return calculateContourResult(segments, null, HouseholdIndex.empty()).stats;
    }

    private static ContourResult calculateContourResult(List<AccessibleSegment> segments, RenderCache renderCache) {
        return calculateContourResult(segments, renderCache, HouseholdIndex.empty());
    }

    private static ContourResult calculateContourResult(
            List<AccessibleSegment> segments,
            RenderCache renderCache,
            HouseholdIndex householdIndex
    ) {
        List<ContourPolygon> polygons = new ArrayList<>();
        double area15 = 0.0;
        double area30 = 0.0;
        double area60 = 0.0;
        double households15 = 0.0;
        double households30 = 0.0;
        double households60 = 0.0;
        for (double threshold : CONTOUR_THRESHOLDS_MINUTES) {
            ContourThresholdResult thresholdResult = renderCache == null
                    ? contourThresholdResult(segments, threshold, householdIndex)
                    : contourThresholdResult(segments, threshold, renderCache, householdIndex);
            polygons.addAll(thresholdResult.polygons);
            if (threshold <= 15.0) {
                area15 = thresholdResult.areaSquareKm;
                households15 = thresholdResult.households;
            } else if (threshold <= 30.0) {
                area30 = thresholdResult.areaSquareKm;
                households30 = thresholdResult.households;
            } else if (threshold <= 60.0) {
                area60 = thresholdResult.areaSquareKm;
                households60 = thresholdResult.households;
            }
        }
        return new ContourResult(new ContourStats(area15, area30, area60, households15, households30, households60), polygons);
    }

    private static ContourThresholdResult contourThresholdResult(
            List<AccessibleSegment> segments,
            double thresholdMinutes,
            RenderCache renderCache,
            HouseholdIndex householdIndex
    ) {
        if (segments.stream().anyMatch(segment -> segment.segmentIndex < 0)
                || renderCache.contourCellOffsets.length < renderCache.segmentCount + 1) {
            return contourThresholdResult(segments, thresholdMinutes, householdIndex);
        }
        Set<Long> occupiedCells = new HashSet<>();
        double cellMeters = Math.max(25.0, SAMPLE_METERS);
        for (AccessibleSegment segment : segments) {
            if (segment.totalMinutes > thresholdMinutes || segment.segmentIndex >= renderCache.segmentCount) {
                continue;
            }
            int start = renderCache.contourCellOffsets[segment.segmentIndex];
            int end = renderCache.contourCellOffsets[segment.segmentIndex + 1];
            for (int i = start; i < end; i++) {
                occupiedCells.add(renderCache.contourCellKeys[i]);
            }
        }
        List<ContourPolygon> polygons = new ArrayList<>();
        double areaSquareMeters = 0.0;
        for (Set<Long> component : contourComponentsLong(occupiedCells)) {
            List<MeterPoint> hull = contourHullLong(component, cellMeters);
            if (hull.size() < 3) {
                continue;
            }
            double componentArea = Math.abs(shoelaceSquareMeters(hull));
            if (componentArea < cellMeters * cellMeters) {
                continue;
            }
            areaSquareMeters += componentArea;
            polygons.add(new ContourPolygon(thresholdMinutes, hull.stream()
                    .map(BusAccessibilityMapCacheJob::meterPointToGeoPoint)
                    .collect(Collectors.toList())));
        }
        return new ContourThresholdResult(areaSquareMeters / 1_000_000.0, householdIndex.sumHouseholds(occupiedCells), polygons);
    }

    private static ContourThresholdResult contourThresholdResult(
            List<AccessibleSegment> segments,
            double thresholdMinutes,
            HouseholdIndex householdIndex
    ) {
        Set<String> occupiedCells = new HashSet<>();
        Set<Long> occupiedCellKeys = new HashSet<>();
        double cellMeters = Math.max(25.0, SAMPLE_METERS);
        int radiusCells = Math.max(0, (int) Math.ceil(CONTOUR_ENVELOPE_METERS / cellMeters));
        for (AccessibleSegment segment : segments) {
            if (segment.totalMinutes > thresholdMinutes) {
                continue;
            }
            int steps = Math.max(1, (int) Math.ceil(segment.lengthMeters / cellMeters));
            for (int step = 0; step <= steps; step++) {
                double t = step / (double) steps;
                Point point = interpolate(segment.from, segment.to, t);
                addContourAreaCells(occupiedCells, point, cellMeters, radiusCells);
                addContourAreaCellKeys(occupiedCellKeys, point, cellMeters, radiusCells);
            }
        }
        List<ContourPolygon> polygons = new ArrayList<>();
        double areaSquareMeters = 0.0;
        for (Set<String> component : contourComponents(occupiedCells)) {
            List<MeterPoint> hull = contourHull(component, cellMeters);
            if (hull.size() < 3) {
                continue;
            }
            double componentArea = Math.abs(shoelaceSquareMeters(hull));
            if (componentArea < cellMeters * cellMeters) {
                continue;
            }
            areaSquareMeters += componentArea;
            polygons.add(new ContourPolygon(thresholdMinutes, hull.stream()
                    .map(BusAccessibilityMapCacheJob::meterPointToGeoPoint)
                    .collect(Collectors.toList())));
        }
        return new ContourThresholdResult(areaSquareMeters / 1_000_000.0, householdIndex.sumHouseholds(occupiedCellKeys), polygons);
    }

    private static void addContourAreaCells(Set<String> occupiedCells, Point point, double cellMeters, int radiusCells) {
        long centerX = contourAreaCellX(point, cellMeters);
        long centerY = contourAreaCellY(point, cellMeters);
        for (int dy = -radiusCells; dy <= radiusCells; dy++) {
            for (int dx = -radiusCells; dx <= radiusCells; dx++) {
                if (Math.hypot(dx, dy) <= radiusCells + 0.25) {
                    occupiedCells.add((centerX + dx) + "|" + (centerY + dy));
                }
            }
        }
    }

    private static void addContourAreaCellKeys(Set<Long> occupiedCells, Point point, double cellMeters, int radiusCells) {
        long centerX = contourAreaCellX(point, cellMeters);
        long centerY = contourAreaCellY(point, cellMeters);
        for (int dy = -radiusCells; dy <= radiusCells; dy++) {
            for (int dx = -radiusCells; dx <= radiusCells; dx++) {
                if (Math.hypot(dx, dy) <= radiusCells + 0.25) {
                    occupiedCells.add(contourCellKey(centerX + dx, centerY + dy));
                }
            }
        }
    }

    private static long contourCellKey(long x, long y) {
        return (x << 32) ^ (y & 0xffffffffL);
    }

    private static long contourCellKeyX(long key) {
        return key >> 32;
    }

    private static long contourCellKeyY(long key) {
        return (int) key;
    }

    private static long contourAreaCellX(Point point, double cellMeters) {
        double lonMeters = point.lon * 111_320.0 * Math.cos(Math.toRadians(CONTOUR_PROJECTION_LAT));
        return (long) Math.floor(lonMeters / cellMeters);
    }

    private static long contourAreaCellY(Point point, double cellMeters) {
        double latMeters = point.lat * 111_320.0;
        return (long) Math.floor(latMeters / cellMeters);
    }

    private static List<Set<String>> contourComponents(Set<String> occupiedCells) {
        List<Set<String>> components = new ArrayList<>();
        Set<String> remaining = new HashSet<>(occupiedCells);
        while (!remaining.isEmpty()) {
            String start = remaining.iterator().next();
            Set<String> component = new HashSet<>();
            ArrayDeque<String> queue = new ArrayDeque<>();
            queue.add(start);
            remaining.remove(start);
            while (!queue.isEmpty()) {
                String current = queue.removeFirst();
                component.add(current);
                long[] xy = parseCellKey(current);
                for (int dy = -1; dy <= 1; dy++) {
                    for (int dx = -1; dx <= 1; dx++) {
                        if (dx == 0 && dy == 0) {
                            continue;
                        }
                        String next = (xy[0] + dx) + "|" + (xy[1] + dy);
                        if (remaining.remove(next)) {
                            queue.add(next);
                        }
                    }
                }
            }
            components.add(component);
        }
        return components;
    }

    private static List<Set<Long>> contourComponentsLong(Set<Long> occupiedCells) {
        List<Set<Long>> components = new ArrayList<>();
        Set<Long> remaining = new HashSet<>(occupiedCells);
        while (!remaining.isEmpty()) {
            long start = remaining.iterator().next();
            Set<Long> component = new HashSet<>();
            ArrayDeque<Long> queue = new ArrayDeque<>();
            queue.add(start);
            remaining.remove(start);
            while (!queue.isEmpty()) {
                long current = queue.removeFirst();
                component.add(current);
                long x = contourCellKeyX(current);
                long y = contourCellKeyY(current);
                for (int dy = -1; dy <= 1; dy++) {
                    for (int dx = -1; dx <= 1; dx++) {
                        if (dx == 0 && dy == 0) {
                            continue;
                        }
                        long next = contourCellKey(x + dx, y + dy);
                        if (remaining.remove(next)) {
                            queue.add(next);
                        }
                    }
                }
            }
            components.add(component);
        }
        return components;
    }

    private static long[] parseCellKey(String key) {
        int split = key.indexOf('|');
        return new long[]{
                Long.parseLong(key.substring(0, split)),
                Long.parseLong(key.substring(split + 1))
        };
    }

    private static List<MeterPoint> contourHull(Set<String> component, double cellMeters) {
        List<MeterPoint> corners = new ArrayList<>(component.size() * 4);
        for (String key : component) {
            long[] xy = parseCellKey(key);
            double x0 = xy[0] * cellMeters;
            double y0 = xy[1] * cellMeters;
            double x1 = x0 + cellMeters;
            double y1 = y0 + cellMeters;
            corners.add(new MeterPoint(x0, y0));
            corners.add(new MeterPoint(x0, y1));
            corners.add(new MeterPoint(x1, y0));
            corners.add(new MeterPoint(x1, y1));
        }
        corners.sort(Comparator
                .comparingDouble((MeterPoint point) -> point.x)
                .thenComparingDouble(point -> point.y));
        List<MeterPoint> unique = new ArrayList<>();
        MeterPoint previous = null;
        for (MeterPoint point : corners) {
            if (previous == null || previous.x != point.x || previous.y != point.y) {
                unique.add(point);
                previous = point;
            }
        }
        if (unique.size() <= 3) {
            return unique;
        }
        List<MeterPoint> lower = new ArrayList<>();
        for (MeterPoint point : unique) {
            while (lower.size() >= 2 && cross(lower.get(lower.size() - 2), lower.get(lower.size() - 1), point) <= 0) {
                lower.remove(lower.size() - 1);
            }
            lower.add(point);
        }
        List<MeterPoint> upper = new ArrayList<>();
        for (int i = unique.size() - 1; i >= 0; i--) {
            MeterPoint point = unique.get(i);
            while (upper.size() >= 2 && cross(upper.get(upper.size() - 2), upper.get(upper.size() - 1), point) <= 0) {
                upper.remove(upper.size() - 1);
            }
            upper.add(point);
        }
        lower.remove(lower.size() - 1);
        upper.remove(upper.size() - 1);
        lower.addAll(upper);
        return lower;
    }

    private static List<MeterPoint> contourHullLong(Set<Long> component, double cellMeters) {
        List<MeterPoint> corners = new ArrayList<>(component.size() * 4);
        for (long key : component) {
            long[] xy = new long[]{contourCellKeyX(key), contourCellKeyY(key)};
            double x0 = xy[0] * cellMeters;
            double y0 = xy[1] * cellMeters;
            double x1 = x0 + cellMeters;
            double y1 = y0 + cellMeters;
            corners.add(new MeterPoint(x0, y0));
            corners.add(new MeterPoint(x0, y1));
            corners.add(new MeterPoint(x1, y0));
            corners.add(new MeterPoint(x1, y1));
        }
        corners.sort(Comparator
                .comparingDouble((MeterPoint point) -> point.x)
                .thenComparingDouble(point -> point.y));
        List<MeterPoint> unique = new ArrayList<>();
        MeterPoint previous = null;
        for (MeterPoint point : corners) {
            if (previous == null || Math.abs(previous.x - point.x) > 0.001 || Math.abs(previous.y - point.y) > 0.001) {
                unique.add(point);
                previous = point;
            }
        }
        if (unique.size() <= 1) {
            return unique;
        }
        List<MeterPoint> lower = new ArrayList<>();
        for (MeterPoint point : unique) {
            while (lower.size() >= 2 && cross(lower.get(lower.size() - 2), lower.get(lower.size() - 1), point) <= 0) {
                lower.remove(lower.size() - 1);
            }
            lower.add(point);
        }
        List<MeterPoint> upper = new ArrayList<>();
        for (int i = unique.size() - 1; i >= 0; i--) {
            MeterPoint point = unique.get(i);
            while (upper.size() >= 2 && cross(upper.get(upper.size() - 2), upper.get(upper.size() - 1), point) <= 0) {
                upper.remove(upper.size() - 1);
            }
            upper.add(point);
        }
        lower.remove(lower.size() - 1);
        upper.remove(upper.size() - 1);
        lower.addAll(upper);
        return lower;
    }

    private static double cross(MeterPoint a, MeterPoint b, MeterPoint c) {
        return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
    }

    private static double shoelaceSquareMeters(List<MeterPoint> polygon) {
        double area = 0.0;
        for (int i = 0; i < polygon.size(); i++) {
            MeterPoint a = polygon.get(i);
            MeterPoint b = polygon.get((i + 1) % polygon.size());
            area += a.x * b.y - b.x * a.y;
        }
        return area / 2.0;
    }

    private static Point meterPointToGeoPoint(MeterPoint point) {
        double lat = point.y / 111_320.0;
        double lon = point.x / (111_320.0 * Math.cos(Math.toRadians(CONTOUR_PROJECTION_LAT)));
        return new Point(lat, lon);
    }

    private static void writeContourStats(
            SparkSession spark,
            Path outputDir,
            String originSlug,
            String originLabel,
            List<SnapshotPayload> snapshots
    ) {
        try {
            Files.createDirectories(outputDir);
            List<Row> rows = snapshots.stream()
                    .map(snapshot -> RowFactory.create(
                            originSlug,
                            originLabel,
                            java.sql.Date.valueOf(snapshot.serviceDate),
                            snapshot.departureTime.toString(),
                            Timestamp.from(snapshot.serviceDate.atTime(snapshot.departureTime).atZone(CITY_ZONE).toInstant()),
                            snapshot.id,
                            snapshot.reachability.stopTimes.size(),
                            snapshot.segmentCount,
                            snapshot.contourStats.area15SquareKm,
                            snapshot.contourStats.area30SquareKm,
                            snapshot.contourStats.area60SquareKm,
                            snapshot.contourStats.households15,
                            snapshot.contourStats.households30,
                            snapshot.contourStats.households60
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
                new StructField("originSlug", DataTypes.StringType, false, Metadata.empty()),
                new StructField("originLabel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("serviceDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("departureTime", DataTypes.StringType, false, Metadata.empty()),
                new StructField("departureAt", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("snapshotId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("reachableStops", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("segmentCount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("area15SquareKm", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("area30SquareKm", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("area60SquareKm", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("households15", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("households30", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("households60", DataTypes.DoubleType, false, Metadata.empty())
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
        payload.put("tileUrlPrefix", first.tileUrlPrefix);
        payload.put("walkSpeedMps", WALK_SPEED_MPS);
        payload.put("walkRadiusMeters", WALK_RADIUS_METERS);
        payload.put("effectiveWalkRadiusMeters", EFFECTIVE_WALK_RADIUS_METERS);
        payload.put("stopClusterRadiusMeters", STOP_CLUSTER_RADIUS_METERS);
        payload.put("sampleMeters", SAMPLE_METERS);
        payload.put("maxRides", MAX_RIDES);
        payload.put("maxCandidateEventsPerStop", MAX_CANDIDATE_EVENTS_PER_STOP);
        payload.put("reachableStops", reachability.stopTimes.size());
        payload.put("roadWays", roads.size());
        payload.put("segmentCount", first.segmentCount);
        payload.put("tileMinZoom", MIN_ZOOM);
        payload.put("tileMaxZoom", MAX_ZOOM);
        payload.put("overlayTileMaxZoom", OVERLAY_MAX_ZOOM);
        payload.put("tileOverlayTemplate", first.tileOverlayTemplate());
        payload.put("totalNormalizedTileOverlayTemplate", first.totalNormalizedColorScale == null ? null : first.totalNormalizedTileOverlayTemplate());
        payload.put("totalLogTileOverlayTemplate", first.totalLogColorScale == null ? null : first.totalLogTileOverlayTemplate());
        payload.put("walkTileOverlayTemplate", first.walkColorScale == null ? null : first.walkTileOverlayTemplate());
        payload.put("stopTransportTileOverlayTemplate", first.stopTransportColorScale == null ? null : first.stopTransportTileOverlayTemplate());
        payload.put("tileRoot", tileBaseRoot.resolve(first.id).resolve("total").toString());
        payload.put("totalNormalizedTileRoot", tileBaseRoot.resolve(first.id).resolve("total-normalized").toString());
        payload.put("totalLogTileRoot", tileBaseRoot.resolve(first.id).resolve("total-log").toString());
        payload.put("walkTileRoot", tileBaseRoot.resolve(first.id).resolve("walk").toString());
        payload.put("stopTransportTileRoot", tileBaseRoot.resolve(first.id).resolve("stop-transport").toString());
        payload.put("contourThresholdMinutes", List.of(15, 30, 60));
        payload.put("totalColorMinMinutes", round1(first.totalColorScale.minMinutes));
        payload.put("totalColorMaxMinutes", round1(first.totalColorScale.maxMinutes));
        putScale(payload, "totalNormalized", first.totalNormalizedColorScale);
        putScale(payload, "totalLog", first.totalLogColorScale);
        putScale(payload, "walk", first.walkColorScale);
        putScale(payload, "stopTransport", first.stopTransportColorScale);
        payload.put("stops", reachability.stopTimes.stream().map(BusAccessibilityMapCacheJob::stopTimePayload).collect(Collectors.toList()));
        List<Map<String, Object>> snapshotPayloads = snapshots.stream().map(BusAccessibilityMapCacheJob::snapshotPayload).collect(Collectors.toList());
        List<Map<String, Object>> contourAreaSeries = snapshots.stream().map(BusAccessibilityMapCacheJob::contourAreaPayload).collect(Collectors.toList());
        if (Files.isRegularFile(outputFile)) {
            Map<String, Object> existing = MAPPER.readValue(outputFile.toFile(), new TypeReference<>() {
            });
            snapshotPayloads = mergePayloadRows(asMapList(existing.get("snapshots")), snapshotPayloads, "id");
            contourAreaSeries = mergePayloadRows(asMapList(existing.get("contourAreaSeries")), contourAreaSeries, "snapshotId");
        }
        payload.put("serviceDates", snapshotPayloads.stream()
                .map(snapshot -> String.valueOf(snapshot.get("serviceDate")))
                .filter(value -> !value.isBlank() && !"null".equals(value))
                .distinct()
                .sorted()
                .collect(Collectors.toList()));
        payload.put("departureTimes", snapshotPayloads.stream()
                .map(snapshot -> String.valueOf(snapshot.get("departureTime")))
                .filter(value -> !value.isBlank() && !"null".equals(value))
                .distinct()
                .sorted()
                .collect(Collectors.toList()));
        payload.put("requestedServiceDates", requestedServiceDates.stream().map(LocalDate::toString).collect(Collectors.toList()));
        payload.put("requestedDepartureTimes", requestedDepartureTimes.stream().map(LocalTime::toString).collect(Collectors.toList()));
        payload.put("availableModes", first.availableModes());
        payload.put("contourAreaSeries", contourAreaSeries);
        payload.put("snapshots", snapshotPayloads);
        writeJsonAtomic(outputFile, payload);
    }

    private static List<Map<String, Object>> mergePayloadRows(List<Map<String, Object>> existingRows, List<Map<String, Object>> newRows, String idField) {
        Map<String, Map<String, Object>> byKey = new LinkedHashMap<>();
        for (Map<String, Object> row : existingRows) {
            byKey.put(payloadRowKey(row, idField), row);
        }
        for (Map<String, Object> row : newRows) {
            byKey.put(payloadRowKey(row, idField), row);
        }
        return byKey.values().stream()
                .sorted(Comparator
                        .comparing((Map<String, Object> row) -> String.valueOf(row.getOrDefault("serviceDate", "")))
                        .thenComparing(row -> String.valueOf(row.getOrDefault("departureTime", ""))))
                .collect(Collectors.toList());
    }

    private static String payloadRowKey(Map<String, Object> row, String idField) {
        Object id = row.get(idField);
        if (id != null && !String.valueOf(id).isBlank()) {
            return String.valueOf(id);
        }
        return String.valueOf(row.getOrDefault("serviceDate", "")) + "-" + String.valueOf(row.getOrDefault("departureTime", ""));
    }

    private static List<Map<String, Object>> asMapList(Object raw) {
        if (!(raw instanceof List<?>)) {
            return List.of();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Object item : (List<?>) raw) {
            if (item instanceof Map<?, ?>) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                    row.put(String.valueOf(entry.getKey()), entry.getValue());
                }
                rows.add(row);
            }
        }
        return rows;
    }

    private static Map<String, Object> snapshotPayload(SnapshotPayload snapshot) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", snapshot.id);
        payload.put("serviceDate", snapshot.serviceDate.toString());
        payload.put("departureTime", snapshot.departureTime.toString());
        payload.put("reachableStops", snapshot.reachability.stopTimes.size());
        payload.put("segmentCount", snapshot.segmentCount);
        payload.put("overlayTileMaxZoom", OVERLAY_MAX_ZOOM);
        payload.put("tileOverlayTemplate", snapshot.tileOverlayTemplate());
        payload.put("availableModes", snapshot.availableModes());
        payload.put("totalNormalizedTileOverlayTemplate", snapshot.totalNormalizedColorScale == null ? null : snapshot.totalNormalizedTileOverlayTemplate());
        payload.put("totalLogTileOverlayTemplate", snapshot.totalLogColorScale == null ? null : snapshot.totalLogTileOverlayTemplate());
        payload.put("walkTileOverlayTemplate", snapshot.walkColorScale == null ? null : snapshot.walkTileOverlayTemplate());
        payload.put("stopTransportTileOverlayTemplate", snapshot.stopTransportColorScale == null ? null : snapshot.stopTransportTileOverlayTemplate());
        payload.put("totalColorMinMinutes", round1(snapshot.totalColorScale.minMinutes));
        payload.put("totalColorMaxMinutes", round1(snapshot.totalColorScale.maxMinutes));
        putScale(payload, "totalNormalized", snapshot.totalNormalizedColorScale);
        putScale(payload, "totalLog", snapshot.totalLogColorScale);
        putScale(payload, "walk", snapshot.walkColorScale);
        putScale(payload, "stopTransport", snapshot.stopTransportColorScale);
        payload.put("contourArea", contourAreaPayload(snapshot));
        payload.put("contourPolygons", snapshot.contourPolygons.stream()
                .map(BusAccessibilityMapCacheJob::contourPolygonPayload)
                .collect(Collectors.toList()));
        return payload;
    }

    private static Map<String, Object> contourPolygonPayload(ContourPolygon polygon) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("thresholdMinutes", (int) Math.round(polygon.thresholdMinutes));
        payload.put("points", polygon.points.stream().map(point -> {
            List<Double> pair = new ArrayList<>(2);
            pair.add(round6(point.lat));
            pair.add(round6(point.lon));
            return pair;
        }).collect(Collectors.toList()));
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
        payload.put("households15", round1(snapshot.contourStats.households15));
        payload.put("households30", round1(snapshot.contourStats.households30));
        payload.put("households60", round1(snapshot.contourStats.households60));
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

    private static double round6(double value) {
        return Math.round(value * 1_000_000.0) / 1_000_000.0;
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

    private static double pixelYToLat(double pixelY, int zoom) {
        double n = Math.PI - 2.0 * Math.PI * pixelY / (TILE_SIZE * Math.pow(2, zoom));
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    private static int metersToPixels(double meters, double lat, int zoom) {
        double earthCircumferenceMeters = 2.0 * Math.PI * 6_378_137.0;
        double metersPerPixel = Math.cos(Math.toRadians(lat)) * earthCircumferenceMeters / (TILE_SIZE * Math.pow(2, zoom));
        return Math.max(1, (int) Math.round(meters / Math.max(0.1, metersPerPixel)));
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
        private final int segmentIndex;
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
                int segmentIndex,
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
            this.segmentIndex = segmentIndex;
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

    private static final class WalkState {
        private final int nodeId;
        private final double walkMeters;

        private WalkState(int nodeId, double walkMeters) {
            this.nodeId = nodeId;
            this.walkMeters = walkMeters;
        }
    }

    private static final class TemplateEndpoint {
        private final int templateIndex;
        private final double extraMeters;

        private TemplateEndpoint(int templateIndex, double extraMeters) {
            this.templateIndex = templateIndex;
            this.extraMeters = extraMeters;
        }
    }

    private static final class WalkCache {
        private final List<CachedSegmentTemplate> segments;
        private final PrimitiveWalkCache primitive;

        private WalkCache(List<CachedSegmentTemplate> segments, PrimitiveWalkCache primitive) {
            this.segments = segments;
            this.primitive = primitive;
        }
    }

    private static final class PrimitiveWalkCache {
        private final String[] stopIds;
        private final Map<String, Integer> stopIndexById;
        private final int segmentCount;
        private final double[] fromLat;
        private final double[] fromLon;
        private final double[] toLat;
        private final double[] toLon;
        private final float[] lengthMeters;
        private final int[] candidateOffsets;
        private final int[] candidateStopIndexes;
        private final float[] candidateWalkMeters;
        // Reserved for the next step: population/apartment weighted accessibility can be stored per road fragment.
        private final float[] populationWeightBySegment;

        private PrimitiveWalkCache(
                String[] stopIds,
                Map<String, Integer> stopIndexById,
                double[] fromLat,
                double[] fromLon,
                double[] toLat,
                double[] toLon,
                float[] lengthMeters,
                int[] candidateOffsets,
                int[] candidateStopIndexes,
                float[] candidateWalkMeters
        ) {
            this.stopIds = stopIds;
            this.stopIndexById = stopIndexById;
            this.segmentCount = fromLat.length;
            this.fromLat = fromLat;
            this.fromLon = fromLon;
            this.toLat = toLat;
            this.toLon = toLon;
            this.lengthMeters = lengthMeters;
            this.candidateOffsets = candidateOffsets;
            this.candidateStopIndexes = candidateStopIndexes;
            this.candidateWalkMeters = candidateWalkMeters;
            this.populationWeightBySegment = null;
        }
    }

    private static final class HouseholdIndex {
        private static final HouseholdIndex EMPTY = new HouseholdIndex(Map.of(), 0, 0.0);

        private final Map<Long, Double> householdsByCell;
        private final int sourceRows;
        private final double totalHouseholds;

        private HouseholdIndex(Map<Long, Double> householdsByCell, int sourceRows, double totalHouseholds) {
            this.householdsByCell = householdsByCell;
            this.sourceRows = sourceRows;
            this.totalHouseholds = totalHouseholds;
        }

        private static HouseholdIndex empty() {
            return EMPTY;
        }

        private double sumHouseholds(Set<Long> occupiedCells) {
            if (householdsByCell.isEmpty() || occupiedCells.isEmpty()) {
                return 0.0;
            }
            double sum = 0.0;
            for (Long cell : occupiedCells) {
                sum += householdsByCell.getOrDefault(cell, 0.0);
            }
            return sum;
        }
    }

    private static final class RenderCache {
        private final int segmentCount;
        private final List<CachedTile> tiles;
        private final int commandCount;
        private final int[] contourCellOffsets;
        private final long[] contourCellKeys;

        private RenderCache(
                int segmentCount,
                List<CachedTile> tiles,
                int commandCount,
                int[] contourCellOffsets,
                long[] contourCellKeys
        ) {
            this.segmentCount = segmentCount;
            this.tiles = tiles;
            this.commandCount = commandCount;
            this.contourCellOffsets = contourCellOffsets;
            this.contourCellKeys = contourCellKeys;
        }
    }

    private static final class CachedTile {
        private final String key;
        private final int zoom;
        private final int[] commandSegmentIndexes;
        private final short[] x0;
        private final short[] y0;
        private final short[] x1;
        private final short[] y1;
        private final short[] width;

        private CachedTile(
                String key,
                int zoom,
                int[] commandSegmentIndexes,
                short[] x0,
                short[] y0,
                short[] x1,
                short[] y1,
                short[] width
        ) {
            this.key = key;
            this.zoom = zoom;
            this.commandSegmentIndexes = commandSegmentIndexes;
            this.x0 = x0;
            this.y0 = y0;
            this.x1 = x1;
            this.y1 = y1;
            this.width = width;
        }
    }

    private static final class CachedTileBuilder {
        private final String key;
        private final int zoom;
        private final List<Integer> segmentIndexes = new ArrayList<>();
        private final List<Short> x0 = new ArrayList<>();
        private final List<Short> y0 = new ArrayList<>();
        private final List<Short> x1 = new ArrayList<>();
        private final List<Short> y1 = new ArrayList<>();
        private final List<Short> width = new ArrayList<>();

        private CachedTileBuilder(String key, int zoom) {
            this.key = key;
            this.zoom = zoom;
        }

        private void add(int segmentIndex, short x0, short y0, short x1, short y1, int width) {
            this.segmentIndexes.add(segmentIndex);
            this.x0.add(x0);
            this.y0.add(y0);
            this.x1.add(x1);
            this.y1.add(y1);
            this.width.add((short) width);
        }

        private CachedTile build() {
            int count = segmentIndexes.size();
            int[] segmentArray = new int[count];
            short[] x0Array = new short[count];
            short[] y0Array = new short[count];
            short[] x1Array = new short[count];
            short[] y1Array = new short[count];
            short[] widthArray = new short[count];
            for (int i = 0; i < count; i++) {
                segmentArray[i] = segmentIndexes.get(i);
                x0Array[i] = x0.get(i);
                y0Array[i] = y0.get(i);
                x1Array[i] = x1.get(i);
                y1Array[i] = y1.get(i);
                widthArray[i] = width.get(i);
            }
            return new CachedTile(key, zoom, segmentArray, x0Array, y0Array, x1Array, y1Array, widthArray);
        }
    }

    private static final class CachedSegmentTemplate {
        private final Point from;
        private final Point to;
        private final double lengthMeters;
        private final List<WalkCandidate> candidates;

        private CachedSegmentTemplate(Point from, Point to, double lengthMeters, List<WalkCandidate> candidates) {
            this.from = from;
            this.to = to;
            this.lengthMeters = lengthMeters;
            this.candidates = candidates;
        }
    }

    private static final class WalkCandidate {
        private final String stopId;
        private final double walkMeters;

        private WalkCandidate(String stopId, double walkMeters) {
            this.stopId = stopId;
            this.walkMeters = walkMeters;
        }
    }

    private static final class CachedSegmentBuilder {
        private final Point from;
        private final Point to;
        private final int aNodeId;
        private final int bNodeId;
        private final double extraFromA;
        private final double extraFromB;
        private final double lengthMeters;
        private final Map<String, Double> bestWalkMetersByStopId = new HashMap<>();

        private CachedSegmentBuilder(
                Point from,
                Point to,
                int aNodeId,
                int bNodeId,
                double extraFromA,
                double extraFromB,
                double lengthMeters
        ) {
            this.from = from;
            this.to = to;
            this.aNodeId = aNodeId;
            this.bNodeId = bNodeId;
            this.extraFromA = extraFromA;
            this.extraFromB = extraFromB;
            this.lengthMeters = lengthMeters;
        }

        private void addCandidate(String stopId, double walkMeters) {
            bestWalkMetersByStopId.merge(stopId, walkMeters, Math::min);
        }

        private CachedSegmentTemplate build() {
            List<WalkCandidate> candidates = bestWalkMetersByStopId.entrySet().stream()
                    .map(entry -> new WalkCandidate(entry.getKey(), entry.getValue()))
                    .sorted(Comparator.comparingDouble(candidate -> candidate.walkMeters))
                    .collect(Collectors.toList());
            return new CachedSegmentTemplate(from, to, lengthMeters, candidates);
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
                return new ColorScale(metric, 0.0, EFFECTIVE_WALK_RADIUS_METERS / WALK_SPEED_MPS / 60.0);
            }
            if (metric == ColorMetric.TOTAL_NORMALIZED) {
                return new ColorScale(metric, 0.0, TOTAL_NORMALIZED_COLOR_MAX_MINUTES);
            }
            if (metric == ColorMetric.TOTAL_LOG) {
                return new ColorScale(metric, 0.0, TOTAL_LOG_COLOR_MAX_MINUTES, true);
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
        private final List<ContourPolygon> contourPolygons;
        private final ColorScale totalNormalizedColorScale;
        private final ColorScale totalLogColorScale;
        private final ColorScale walkColorScale;
        private final ColorScale stopTransportColorScale;
        private final String tileUrlPrefix;

        private SnapshotPayload(
                String id,
                LocalDate serviceDate,
                LocalTime departureTime,
                Reachability reachability,
                int segmentCount,
                ContourStats contourStats,
                List<ContourPolygon> contourPolygons,
                ColorScale totalColorScale,
                ColorScale totalNormalizedColorScale,
                ColorScale totalLogColorScale,
                ColorScale walkColorScale,
                ColorScale stopTransportColorScale,
                String tileUrlPrefix
        ) {
            this.id = id;
            this.serviceDate = serviceDate;
            this.departureTime = departureTime;
            this.reachability = reachability;
            this.segmentCount = segmentCount;
            this.contourStats = contourStats;
            this.contourPolygons = contourPolygons;
            this.totalColorScale = totalColorScale;
            this.totalNormalizedColorScale = totalNormalizedColorScale;
            this.totalLogColorScale = totalLogColorScale;
            this.walkColorScale = walkColorScale;
            this.stopTransportColorScale = stopTransportColorScale;
            this.tileUrlPrefix = tileUrlPrefix;
        }

        private String tileOverlayTemplate() {
            return "./tiles/" + tileUrlPrefix + "/" + id + "/total/{z}/{x}/{y}.png";
        }

        private String totalLogTileOverlayTemplate() {
            return "./tiles/" + tileUrlPrefix + "/" + id + "/total-log/{z}/{x}/{y}.png";
        }

        private String totalNormalizedTileOverlayTemplate() {
            return "./tiles/" + tileUrlPrefix + "/" + id + "/total-normalized/{z}/{x}/{y}.png";
        }

        private String walkTileOverlayTemplate() {
            return "./tiles/" + tileUrlPrefix + "/" + id + "/walk/{z}/{x}/{y}.png";
        }

        private String stopTransportTileOverlayTemplate() {
            return "./tiles/" + tileUrlPrefix + "/" + id + "/stop-transport/{z}/{x}/{y}.png";
        }

        private List<String> availableModes() {
            List<String> modes = new ArrayList<>();
            modes.add("total");
            if (totalNormalizedColorScale != null) {
                modes.add("totalNormalized");
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
        private final double households15;
        private final double households30;
        private final double households60;

        private ContourStats(
                double area15SquareKm,
                double area30SquareKm,
                double area60SquareKm,
                double households15,
                double households30,
                double households60
        ) {
            this.area15SquareKm = area15SquareKm;
            this.area30SquareKm = area30SquareKm;
            this.area60SquareKm = area60SquareKm;
            this.households15 = households15;
            this.households30 = households30;
            this.households60 = households60;
        }
    }

    private static final class ContourResult {
        private final ContourStats stats;
        private final List<ContourPolygon> polygons;

        private ContourResult(ContourStats stats, List<ContourPolygon> polygons) {
            this.stats = stats;
            this.polygons = polygons;
        }
    }

    private static final class ContourThresholdResult {
        private final double areaSquareKm;
        private final double households;
        private final List<ContourPolygon> polygons;

        private ContourThresholdResult(double areaSquareKm, double households, List<ContourPolygon> polygons) {
            this.areaSquareKm = areaSquareKm;
            this.households = households;
            this.polygons = polygons;
        }
    }

    private static final class ContourPolygon {
        private final double thresholdMinutes;
        private final List<Point> points;

        private ContourPolygon(double thresholdMinutes, List<Point> points) {
            this.thresholdMinutes = thresholdMinutes;
            this.points = points;
        }
    }

    private static final class MeterPoint {
        private final double x;
        private final double y;

        private MeterPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }

    private enum ColorMetric {
        TOTAL {
            @Override
            double value(AccessibleSegment segment) {
                return segment.totalMinutes;
            }
        },
        TOTAL_NORMALIZED {
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
