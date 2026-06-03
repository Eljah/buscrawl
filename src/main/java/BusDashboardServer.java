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
    private final AtomicReference<byte[]> cachedIndexHtml = new AtomicReference<>();
    private final AtomicReference<byte[]> cachedRouteMovementHtml = new AtomicReference<>();
    private final Path statsCacheFile;
    private final Path routeMovementCacheFile;
    private final int port;

    public BusDashboardServer(Path statsCacheFile, Path routeMovementCacheFile, int port) {
        this.statsCacheFile = statsCacheFile;
        this.routeMovementCacheFile = routeMovementCacheFile;
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
        int port = Integer.parseInt(System.getenv().getOrDefault("BUS_DASHBOARD_PORT", "8061"));

        BusDashboardServer server = new BusDashboardServer(statsCacheFile, routeMovementCacheFile, port);
        server.start();
    }

    private void start() throws Exception {
        cachedIndexHtml.set(loadResourceBytes("dashboard/index.html"));
        cachedRouteMovementHtml.set(loadResourceBytes("dashboard/routes-last-movement.html"));
        refreshCache();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::safeRefreshCache, 15, 15, TimeUnit.SECONDS);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/health", new TextHandler("ok\n", "text/plain; charset=utf-8"));
        server.createContext("/api/stats", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedStatsJson.get()));
        server.createContext("/api/route-last-movement", exchange -> writeResponse(exchange, 200, "application/json; charset=utf-8", cachedRouteMovementJson.get()));
        server.createContext("/routes-last-movement", exchange -> writeResponse(exchange, 200, "text/html; charset=utf-8", cachedRouteMovementHtml.get()));
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

    private byte[] loadResourceBytes(String resourcePath) throws IOException {
        try (InputStream inputStream = BusDashboardServer.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException(resourcePath + " not found");
            }
            return inputStream.readAllBytes();
        }
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
