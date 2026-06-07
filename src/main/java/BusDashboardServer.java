import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class BusDashboardServer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));

    private final AtomicReference<byte[]> cachedStatsJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRouteMovementJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedTraceJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedStopLastPassJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedOvertakeJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRubberinessJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedMapConfigJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedIndexHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRouteMovementHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedTraceMapHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedStopLastPassHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedOvertakeHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRubberinessHtml = new AtomicReference<>();
    private final Path statsCacheFile;
    private final Path routeMovementCacheFile;
    private final Path traceCacheFile;
    private final Path stopLastPassCacheFile;
    private final Path overtakeCacheFile;
    private final Path rubberinessCacheFile;
    private final Path mapConfigFile;
    private final Path tileRoot;
    private final Path trafficBehaviorDir;
    private final int port;

    public BusDashboardServer(
            Path statsCacheFile,
            Path routeMovementCacheFile,
            Path traceCacheFile,
            Path stopLastPassCacheFile,
            Path overtakeCacheFile,
            Path rubberinessCacheFile,
            Path mapConfigFile,
            Path tileRoot,
            Path trafficBehaviorDir,
            int port
    ) {
        this.statsCacheFile = statsCacheFile;
        this.routeMovementCacheFile = routeMovementCacheFile;
        this.traceCacheFile = traceCacheFile;
        this.stopLastPassCacheFile = stopLastPassCacheFile;
        this.overtakeCacheFile = overtakeCacheFile;
        this.rubberinessCacheFile = rubberinessCacheFile;
        this.mapConfigFile = mapConfigFile;
        this.tileRoot = tileRoot;
        this.trafficBehaviorDir = trafficBehaviorDir;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        Path statsCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_CACHE_FILE",
                "./var/bus/dashboard-cache/stats.json"
        ));
        Path routeMovementCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_ROUTE_CACHE_FILE",
                statsCacheFile.resolveSibling("route-last-movement.json").toString()
        ));
        Path traceCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_TRACE_CACHE_FILE",
                statsCacheFile.resolveSibling("bus-traces.json").toString()
        ));
        Path stopLastPassCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_STOP_LAST_PASS_CACHE_FILE",
                statsCacheFile.resolveSibling("stop-last-pass.json").toString()
        ));
        Path overtakeCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_OVERTAKE_CACHE_FILE",
                statsCacheFile.resolveSibling("overtake.json").toString()
        ));
        Path rubberinessCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_RUBBERINESS_CACHE_FILE",
                statsCacheFile.resolveSibling("rubberiness.json").toString()
        ));
        Path mapConfigFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_MAP_CONFIG_FILE",
                statsCacheFile.resolveSibling("map-config.json").toString()
        ));
        Path tileRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TILE_ROOT",
                statsCacheFile.resolveSibling("tiles").toString()
        ));
        Path trafficBehaviorDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        int port = Integer.parseInt(System.getenv().getOrDefault("BUS_DASHBOARD_PORT", "8061"));

        BusDashboardServer server = new BusDashboardServer(
                statsCacheFile,
                routeMovementCacheFile,
                traceCacheFile,
                stopLastPassCacheFile,
                overtakeCacheFile,
                rubberinessCacheFile,
                mapConfigFile,
                tileRoot,
                trafficBehaviorDir,
                port
        );
        server.start();
    }

    private void start() throws Exception {
        cachedIndexHtml.set(loadResourceBytes("dashboard/index.html"));
        cachedRouteMovementHtml.set(loadResourceBytes("dashboard/routes-last-movement.html"));
        cachedTraceMapHtml.set(loadResourceBytes("dashboard/bus-traces-map.html"));
        cachedStopLastPassHtml.set(loadResourceBytes("dashboard/stop-last-pass.html"));
        cachedOvertakeHtml.set(loadResourceBytes("dashboard/overtakes.html"));
        cachedRubberinessHtml.set(loadResourceBytes("dashboard/rubberiness.html"));
        refreshCache();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::safeRefreshCache, 15, 15, TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/health", new TextHandler("ok\n", "text/plain; charset=utf-8"));
        server.createContext("/api/stats", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedStatsJson.get()));
        server.createContext("/api/route-last-movement", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedRouteMovementJson.get()));
        server.createContext("/api/bus-traces", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedTraceJson.get()));
        server.createContext("/api/stop-last-pass", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedStopLastPassJson.get()));
        server.createContext("/api/overtakes", this::handleOvertakesRequest);
        server.createContext("/api/overtake-details", this::handleOvertakeDetailsRequest);
        server.createContext("/api/rubberiness", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedRubberinessJson.get()));
        server.createContext("/api/map-config", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedMapConfigJson.get()));
        server.createContext("/routes-last-movement", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedRouteMovementHtml.get()));
        server.createContext("/bus-traces-map", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedTraceMapHtml.get()));
        server.createContext("/stop-last-pass", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedStopLastPassHtml.get()));
        server.createContext("/overtakes", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedOvertakeHtml.get()));
        server.createContext("/rubberiness", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedRubberinessHtml.get()));
        server.createContext("/tiles", this::handleTileRequest);
        server.createContext("/", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedIndexHtml.get()));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        System.out.println("Dashboard server started on 127.0.0.1:" + port);
    }

    private void safeRefreshCache() {
        try {
            refreshCache();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void refreshCache() throws IOException {
        cachedStatsJson.set(loadJsonCache(statsCacheFile, emptyStatsPayload(
                "Stats cache file not found: " + statsCacheFile
        )));
        cachedRouteMovementJson.set(loadJsonCache(routeMovementCacheFile, emptyRouteMovementPayload(
                "Route cache file not found: " + routeMovementCacheFile
        )));
        cachedTraceJson.set(loadJsonCache(traceCacheFile, emptyTracePayload(
                "Trace cache file not found: " + traceCacheFile
        )));
        cachedStopLastPassJson.set(loadJsonCache(stopLastPassCacheFile, emptyStopLastPassPayload(
                "Stop last-pass cache file not found: " + stopLastPassCacheFile
        )));
        cachedOvertakeJson.set(loadJsonCache(overtakeCacheFile, emptyOvertakePayload(
                "Overtake cache file not found: " + overtakeCacheFile
        )));
        cachedRubberinessJson.set(loadJsonCache(rubberinessCacheFile, emptyRubberinessPayload(
                "Rubberiness cache file not found: " + rubberinessCacheFile
        )));
        cachedMapConfigJson.set(loadJsonCache(mapConfigFile, emptyMapConfigPayload(
                "Map config file not found: " + mapConfigFile
        )));
    }

    private byte[] loadJsonCache(Path path, Map<String, Object> fallbackPayload) throws IOException {
        if (Files.exists(path)) {
            return Files.readAllBytes(path);
        }
        return MAPPER.writeValueAsBytes(fallbackPayload);
    }

    private static Map<String, Object> emptyStatsPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("totalPoints", 0);
        payload.put("bucket", "5 minutes");
        payload.put("downsampled", false);
        payload.put("rangeStart", null);
        payload.put("rangeEnd", null);
        payload.put("series", List.of());
        return payload;
    }

    private static Map<String, Object> emptyRouteMovementPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("today", null);
        payload.put("yesterday", null);
        payload.put("routes", List.of());
        return payload;
    }

    private static Map<String, Object> emptyTracePayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("windowMinutes", 10);
        payload.put("maxPointsPerBus", 100);
        payload.put("busCount", 0);
        payload.put("buses", List.of());
        return payload;
    }

    private static Map<String, Object> emptyMapConfigPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("tileSize", 256);
        payload.put("minZoom", 11);
        payload.put("maxZoom", 17);
        payload.put("initialZoom", 12);
        payload.put("tileUrlTemplate", "./tiles/base/{z}/{x}/{y}.png");
        payload.put("bounds", null);
        payload.put("center", null);
        return payload;
    }

    private static Map<String, Object> emptyStopLastPassPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("timezone", "Europe/Moscow");
        payload.put("yesterday", null);
        payload.put("weekdays", List.of());
        payload.put("routeShapes", List.of());
        payload.put("stops", List.of());
        payload.put("routes", Map.of(
                "previousDay", List.of(),
                "allDaysAverage", List.of(),
                "weekdayAverage", Map.of()
        ));
        payload.put("stopsData", Map.of(
                "previousDay", List.of(),
                "allDaysAverage", List.of(),
                "weekdayAverage", Map.of()
        ));
        return payload;
    }

    private static Map<String, Object> emptyOvertakePayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("timezone", "Europe/Moscow");
        payload.put("yesterday", null);
        payload.put("weekdays", List.of());
        payload.put("segmentShapes", List.of());
        payload.put("routeShapes", List.of());
        Map<String, Object> emptySection = Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        );
        payload.put("segmentData", emptySection);
        payload.put("segmentRouteData", emptySection);
        payload.put("segmentVehicleData", emptySection);
        payload.put("vehicleData", emptySection);
        payload.put("pointHeatmapData", emptySection);
        payload.put("physicalPointHeatmapData", emptySection);
        return payload;
    }

    private static Map<String, Object> emptyRubberinessPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("timezone", "Europe/Moscow");
        payload.put("yesterday", null);
        payload.put("weekdays", List.of());
        payload.put("routeShapes", List.of());
        payload.put("stops", List.of());
        payload.put("terminalPolicies", List.of());
        Map<String, Object> emptySection = Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        );
        payload.put("stopData", emptySection);
        payload.put("stopRouteData", emptySection);
        payload.put("routeData", emptySection);
        payload.put("vehicleData", emptySection);
        return payload;
    }

    private byte[] loadResourceBytes(String resourcePath) throws IOException {
        try (InputStream inputStream = BusDashboardServer.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException(resourcePath + " not found");
            }
            return inputStream.readAllBytes();
        }
    }

    private void handleOvertakesRequest(HttpExchange exchange) throws IOException {
        String rawQuery = exchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            writeResponse(exchange, 200, "application/json; charset=utf-8", cachedOvertakeJson.get());
            return;
        }

        Map<String, String> query = parseQuery(rawQuery);
        Map<String, Object> source = MAPPER.readValue(cachedOvertakeJson.get(), new TypeReference<>() {
        });
        Map<String, Object> payload = new LinkedHashMap<>();
        copyIfPresent(source, payload, "updatedAt");
        copyIfPresent(source, payload, "status");
        copyIfPresent(source, payload, "message");
        copyIfPresent(source, payload, "timezone");
        copyIfPresent(source, payload, "yesterday");
        copyIfPresent(source, payload, "weekdays");

        String scope = query.getOrDefault("scope", "route");
        String mode = query.getOrDefault("mode", "segment");
        String period = query.getOrDefault("period", "previous");
        String weekday = query.getOrDefault("weekday", "1");
        String sectionKey = overtakeSectionKey(scope, mode);
        payload.put(sectionKey, compactOvertakeSection(source.get(sectionKey), period, weekday));
        try {
            payload.put("timeline", loadOvertakeTimeline(query, scope, null, null));
        } catch (Exception e) {
            payload.put("timeline", List.of());
        }

        if (("vehicle".equals(mode))) {
            copyIfPresent(source, payload, "routeShapes");
        } else if (!"physical".equals(scope) && !"pointHeatmap".equals(mode) && !"pointTileHeatmap".equals(mode)) {
            copyIfPresent(source, payload, "segmentShapes");
        }
        if (!payload.containsKey("segmentShapes")) {
            payload.put("segmentShapes", List.of());
        }
        if (!payload.containsKey("routeShapes")) {
            payload.put("routeShapes", List.of());
        }

        writeResponse(exchange, 200, "application/json; charset=utf-8", MAPPER.writeValueAsBytes(payload));
    }

    private static String overtakeSectionKey(String scope, String mode) {
        if ("pointHeatmap".equals(mode) || "pointTileHeatmap".equals(mode)) {
            return "physical".equals(scope) ? "physicalPointHeatmapData" : "pointHeatmapData";
        }
        if ("physical".equals(scope)) {
            if ("vehicle".equals(mode)) {
                return "physicalVehicleData";
            }
            if ("segmentVehicle".equals(mode)) {
                return "physicalSegmentVehicleData";
            }
            return "physicalSegmentData";
        }
        if ("segmentRoute".equals(mode)) {
            return "segmentRouteData";
        }
        if ("segmentVehicle".equals(mode)) {
            return "segmentVehicleData";
        }
        if ("vehicle".equals(mode)) {
            return "vehicleData";
        }
        return "segmentData";
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> compactOvertakeSection(Object sectionObject, String period, String weekday) {
        Map<String, Object> compact = new LinkedHashMap<>();
        compact.put("previousDay", List.of());
        compact.put("allDays", List.of());
        compact.put("weekday", Map.of());
        if (!(sectionObject instanceof Map)) {
            return compact;
        }

        Map<String, Object> section = (Map<String, Object>) sectionObject;
        if ("all".equals(period)) {
            compact.put("allDays", section.getOrDefault("allDays", List.of()));
        } else if ("weekday".equals(period)) {
            Object weekdayObject = section.get("weekday");
            Object rows = List.of();
            if (weekdayObject instanceof Map) {
                rows = ((Map<String, Object>) weekdayObject).getOrDefault(weekday, List.of());
            }
            compact.put("weekday", Map.of(weekday, rows));
        } else {
            compact.put("previousDay", section.getOrDefault("previousDay", List.of()));
        }
        return compact;
    }

    private static void copyIfPresent(Map<String, Object> source, Map<String, Object> target, String key) {
        if (source.containsKey(key)) {
            target.put(key, source.get(key));
        }
    }

    private void handleOvertakeDetailsRequest(HttpExchange exchange) throws IOException {
        try {
            Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
            String scope = query.getOrDefault("scope", "route");
            String plate = query.getOrDefault("plate", "").trim();
            if (plate.isEmpty()) {
                writeResponse(exchange, 400, "application/json; charset=utf-8", errorPayload("Missing plate parameter"));
                return;
            }

            Map<String, Object> detailsPayload = loadOvertakeDetails(query, scope, plate);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
            payload.put("status", "ok");
            payload.put("scope", scope);
            payload.put("plate", plate);
            payload.putAll(detailsPayload);
            writeResponse(exchange, 200, "application/json; charset=utf-8", MAPPER.writeValueAsBytes(payload));
        } catch (Exception e) {
            writeResponse(exchange, 500, "application/json; charset=utf-8", errorPayload(e.getMessage()));
        }
    }

    private Map<String, Object> loadOvertakeDetails(Map<String, String> query, String scope, String plate) throws Exception {
        boolean physical = "physical".equalsIgnoreCase(scope);
        Path eventsDir = trafficBehaviorDir.resolve(physical ? "physical-overtake-events" : "overtake-events");
        if (!Files.exists(eventsDir)) {
            return Map.of("details", List.of(), "timeline", List.of(), "page", 1, "pageSize", 50, "totalEvents", 0, "hasMore", false);
        }

        String segmentColumn = physical ? "physicalSegmentId" : "segmentId";
        String routeColumn = physical ? "overtakenRouteNumber" : "routeNumber";
        int page = Math.max(1, parsePositiveInt(query.get("page"), 1));
        int pageSize = Math.min(50, Math.max(1, parsePositiveInt(query.get("pageSize"), 50)));
        int offset = (page - 1) * pageSize;

        FilterSpec filter = buildOvertakeFilter(query, eventsDir, segmentColumn, plate);
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append("CAST(serviceDate AS VARCHAR), weekdayIso, overtakerPlate, overtakerRouteNumber, ");
        sql.append("overtakenPlate, ").append(routeColumn).append(", ").append(segmentColumn).append(", ");
        sql.append("startStopName, endStopName, startStopLatitude, startStopLongitude, endStopLatitude, endStopLongitude, ");
        sql.append("distanceMeters, overtakeAt, overtakerTravelDurationSeconds, overtakenTravelDurationSeconds, ");
        sql.append("overtakerAvgSegmentSpeedKmh, overtakenAvgSegmentSpeedKmh ");
        sql.append("FROM read_parquet(?) WHERE ").append(filter.whereClause).append(" ");
        sql.append("ORDER BY overtakeAt DESC LIMIT ? OFFSET ?");

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            List<Map<String, Object>> rows = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                bindParameters(statement, filter.parameters);
                statement.setInt(filter.parameters.size() + 1, pageSize);
                statement.setInt(filter.parameters.size() + 2, offset);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("serviceDate", rs.getString(1));
                    row.put("weekdayIso", rs.getInt(2));
                    row.put("overtakerPlate", rs.getString(3));
                    row.put("overtakerRouteNumber", rs.getString(4));
                    row.put("overtakenPlate", rs.getString(5));
                    row.put("overtakenRouteNumber", rs.getString(6));
                    row.put("segmentId", rs.getString(7));
                    if (physical) {
                        row.put("physicalSegmentId", rs.getString(7));
                    }
                    row.put("startStopName", rs.getString(8));
                    row.put("endStopName", rs.getString(9));
                    row.put("startStopLatitude", rs.getDouble(10));
                    row.put("startStopLongitude", rs.getDouble(11));
                    row.put("endStopLatitude", rs.getDouble(12));
                    row.put("endStopLongitude", rs.getDouble(13));
                    row.put("distanceMeters", rs.getDouble(14));
                    row.put("overtakeAt", toIsoString(rs.getObject(15)));
                    row.put("overtakerTravelDurationSeconds", rs.getLong(16));
                    row.put("overtakenTravelDurationSeconds", rs.getLong(17));
                    row.put("overtakerAvgSegmentSpeedKmh", rs.getDouble(18));
                    row.put("overtakenAvgSegmentSpeedKmh", rs.getDouble(19));
                    rows.add(row);
                }
            }
        }
            long totalEvents = countOvertakeEvents(connection, filter);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("details", rows);
            payload.put("timeline", loadOvertakeTimeline(connection, filter));
            payload.put("page", page);
            payload.put("pageSize", pageSize);
            payload.put("totalEvents", totalEvents);
            payload.put("hasMore", offset + rows.size() < totalEvents);
            return payload;
        }
    }

    private List<Map<String, Object>> loadOvertakeTimeline(Map<String, String> query, String scope, String plate, String segment) throws Exception {
        boolean physical = "physical".equalsIgnoreCase(scope);
        Path eventsDir = trafficBehaviorDir.resolve(physical ? "physical-overtake-events" : "overtake-events");
        if (!Files.exists(eventsDir)) {
            return List.of();
        }
        Map<String, String> timelineQuery = new LinkedHashMap<>(query);
        if (segment != null && !segment.isBlank()) {
            timelineQuery.put("segment", segment);
        }
        String segmentColumn = physical ? "physicalSegmentId" : "segmentId";
        FilterSpec filter = buildOvertakeFilter(timelineQuery, eventsDir, segmentColumn, plate);
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            return loadOvertakeTimeline(connection, filter);
        }
    }

    private List<Map<String, Object>> loadOvertakeTimeline(Connection connection, FilterSpec filter) throws Exception {
        String sql = "SELECT time_bucket(INTERVAL '5 minutes', overtakeAt) AS bucketTime, COUNT(*) "
                + "FROM read_parquet(?) WHERE " + filter.whereClause + " AND overtakeAt IS NOT NULL "
                + "GROUP BY bucketTime ORDER BY bucketTime";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindParameters(statement, filter.parameters);
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("bucketTime", toIsoString(rs.getObject(1)));
                    row.put("count", rs.getLong(2));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private long countOvertakeEvents(Connection connection, FilterSpec filter) throws Exception {
        String sql = "SELECT COUNT(*) FROM read_parquet(?) WHERE " + filter.whereClause;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindParameters(statement, filter.parameters);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private FilterSpec buildOvertakeFilter(Map<String, String> query, Path eventsDir, String segmentColumn, String plate) {
        List<String> clauses = new ArrayList<>();
        List<Object> parameters = new ArrayList<>();
        clauses.add("TRUE");
        parameters.add(parquetGlob(eventsDir));
        if (plate != null && !plate.isBlank()) {
            clauses.add("overtakerPlate = ?");
            parameters.add(plate);
        }
        String segment = query.getOrDefault("segment", "").trim();
        if (!segment.isEmpty()) {
            clauses.add(segmentColumn + " = ?");
            parameters.add(segment);
        }
        String period = query.getOrDefault("period", "all");
        if ("previous".equals(period)) {
            String serviceDate = query.getOrDefault("serviceDate", "").trim();
            if (!serviceDate.isEmpty()) {
                clauses.add("CAST(serviceDate AS VARCHAR) = ?");
                parameters.add(serviceDate);
            }
        } else {
            clauses.add("CAST(serviceDate AS VARCHAR) < ?");
            parameters.add(LocalDate.now(CITY_ZONE).toString());
            if ("weekday".equals(period)) {
                String weekday = query.getOrDefault("weekday", "").trim();
                if (!weekday.isEmpty()) {
                    clauses.add("weekdayIso = ?");
                    parameters.add(Integer.parseInt(weekday));
                }
            }
        }
        return new FilterSpec(String.join(" AND ", clauses), parameters);
    }

    private static void bindParameters(PreparedStatement statement, List<Object> parameters) throws Exception {
        for (int i = 0; i < parameters.size(); i++) {
            statement.setObject(i + 1, parameters.get(i));
        }
    }

    private static int parsePositiveInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> result = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            int separator = pair.indexOf('=');
            String key = separator >= 0 ? pair.substring(0, separator) : pair;
            String value = separator >= 0 ? pair.substring(separator + 1) : "";
            result.put(urlDecode(key), urlDecode(value));
        }
        return result;
    }

    private static String urlDecode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static byte[] errorPayload(String message) throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
        payload.put("status", "error");
        payload.put("message", message == null ? "Unexpected error" : message);
        return MAPPER.writeValueAsBytes(payload);
    }

    private static String parquetGlob(Path dir) {
        return dir.toAbsolutePath().toString().replace('\\', '/') + "/**/*.parquet";
    }

    private static String toIsoString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant().toString();
        }
        return String.valueOf(value);
    }

    private void handleTileRequest(HttpExchange exchange) throws IOException {
        String contextPath = exchange.getHttpContext().getPath();
        String requestPath = exchange.getRequestURI().getPath();
        String suffix = requestPath.substring(contextPath.length());
        if (suffix.startsWith("/")) {
            suffix = suffix.substring(1);
        }
        if (suffix.isEmpty() || suffix.contains("..")) {
            writeResponse(exchange, 404, "text/plain; charset=utf-8", "Tile not found\n".getBytes(StandardCharsets.UTF_8));
            return;
        }

        Path tilePath = tileRoot.resolve(suffix).normalize();
        if (!tilePath.startsWith(tileRoot) || !Files.isRegularFile(tilePath)) {
            writeResponse(exchange, 404, "text/plain; charset=utf-8", "Tile not found\n".getBytes(StandardCharsets.UTF_8));
            return;
        }

        byte[] body = Files.readAllBytes(tilePath);
        exchange.getResponseHeaders().set("Content-Type", "image/png");
        exchange.getResponseHeaders().set("Cache-Control", "public, max-age=86400");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static void writeResponse(HttpExchange exchange, int status, String contentType, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
        exchange.close();
    }

    private static final class TextHandler implements HttpHandler {
        private final byte[] body;
        private final String contentType;

        private TextHandler(String body, String contentType) {
            this.body = body.getBytes(StandardCharsets.UTF_8);
            this.contentType = contentType;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            writeResponse(exchange, 200, contentType, body);
        }
    }

    private static final class FilterSpec {
        private final String whereClause;
        private final List<Object> parameters;

        private FilterSpec(String whereClause, List<Object> parameters) {
            this.whereClause = whereClause;
            this.parameters = parameters;
        }
    }
}
