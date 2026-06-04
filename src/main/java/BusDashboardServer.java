import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class BusDashboardServer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final AtomicReference<byte[]> cachedStatsJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRouteMovementJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedTraceJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedStopLastPassJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedMapConfigJson = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedIndexHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRouteMovementHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedTraceMapHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedStopLastPassHtml = new AtomicReference<>();
    private final Path statsCacheFile;
    private final Path routeMovementCacheFile;
    private final Path traceCacheFile;
    private final Path stopLastPassCacheFile;
    private final Path mapConfigFile;
    private final Path tileRoot;
    private final int port;

    public BusDashboardServer(
            Path statsCacheFile,
            Path routeMovementCacheFile,
            Path traceCacheFile,
            Path stopLastPassCacheFile,
            Path mapConfigFile,
            Path tileRoot,
            int port
    ) {
        this.statsCacheFile = statsCacheFile;
        this.routeMovementCacheFile = routeMovementCacheFile;
        this.traceCacheFile = traceCacheFile;
        this.stopLastPassCacheFile = stopLastPassCacheFile;
        this.mapConfigFile = mapConfigFile;
        this.tileRoot = tileRoot;
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
        Path mapConfigFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_MAP_CONFIG_FILE",
                statsCacheFile.resolveSibling("map-config.json").toString()
        ));
        Path tileRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TILE_DIR",
                statsCacheFile.resolveSibling("tiles/base").toString()
        ));
        int port = Integer.parseInt(System.getenv().getOrDefault("BUS_DASHBOARD_PORT", "8061"));

        BusDashboardServer server = new BusDashboardServer(
                statsCacheFile,
                routeMovementCacheFile,
                traceCacheFile,
                stopLastPassCacheFile,
                mapConfigFile,
                tileRoot,
                port
        );
        server.start();
    }

    private void start() throws Exception {
        cachedIndexHtml.set(loadResourceBytes("dashboard/index.html"));
        cachedRouteMovementHtml.set(loadResourceBytes("dashboard/routes-last-movement.html"));
        cachedTraceMapHtml.set(loadResourceBytes("dashboard/bus-traces-map.html"));
        cachedStopLastPassHtml.set(loadResourceBytes("dashboard/stop-last-pass.html"));
        refreshCache();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::safeRefreshCache, 15, 15, TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/health", new TextHandler("ok\n", "text/plain; charset=utf-8"));
        server.createContext("/api/stats", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedStatsJson.get()));
        server.createContext("/api/route-last-movement", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedRouteMovementJson.get()));
        server.createContext("/api/bus-traces", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedTraceJson.get()));
        server.createContext("/api/stop-last-pass", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedStopLastPassJson.get()));
        server.createContext("/api/map-config", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedMapConfigJson.get()));
        server.createContext("/routes-last-movement", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedRouteMovementHtml.get()));
        server.createContext("/bus-traces-map", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedTraceMapHtml.get()));
        server.createContext("/stop-last-pass", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedStopLastPassHtml.get()));
        server.createContext("/tiles/base", this::handleTileRequest);
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

    private byte[] loadResourceBytes(String resourcePath) throws IOException {
        try (InputStream inputStream = BusDashboardServer.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException(resourcePath + " not found");
            }
            return inputStream.readAllBytes();
        }
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
}
