import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class BusActiveTraceCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TRACE_WINDOW = Duration.ofMinutes(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_WINDOW_MINUTES", "10"))
    );
    private static final Duration WRITE_INTERVAL = Duration.ofSeconds(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_WRITE_INTERVAL_SECONDS", "5"))
    );
    private static final int MAX_POINTS_PER_BUS = Integer.parseInt(
            System.getenv().getOrDefault("BUS_TRACE_POINTS_LIMIT", "100")
    );
    private static final Duration FILE_LOOKBACK_SLACK = Duration.ofMinutes(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_FILE_LOOKBACK_MINUTES", "30"))
    );

    public static void main(String[] args) throws Exception {
        Path parquetDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path traceCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_TRACE_CACHE_FILE",
                "./var/bus/dashboard-cache/bus-traces.json"
        ));
        Path mapConfigFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_MAP_CONFIG_FILE",
                traceCacheFile.resolveSibling("map-config.json").toString()
        ));
        Path tileRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TILE_ROOT",
                traceCacheFile.resolveSibling("tiles").toString()
        ));

        Files.createDirectories(traceCacheFile.getParent());
        Files.createDirectories(tileRoot);

        if (!Files.isDirectory(parquetDir)) {
            writeJsonAtomic(traceCacheFile, emptyPayload("Parquet directory not found: " + parquetDir));
            return;
        }

        RouteMapper routeMapper = new RouteMapper(null);
        SparkSession spark = SparkSession.builder()
                .appName("BusActiveTraceCacheJob")
                .master(System.getenv().getOrDefault("BUS_TRACE_SPARK_MASTER", "local[2]"))
                .config("spark.local.dir", System.getenv().getOrDefault("BUS_TRACE_SPARK_LOCAL_DIR", "./var/bus/trace-cache-spark-temp"))
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_TRACE_SPARK_DRIVER_MEMORY", "2g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_TRACE_SPARK_EXECUTOR_MEMORY", "2g"))
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            while (true) {
                Instant now = Instant.now();
                TraceStore traceStore = new TraceStore(MAX_POINTS_PER_BUS, TRACE_WINDOW);
                rebuildRecentTraceStore(spark, parquetDir, routeMapper, traceStore, now);
                writeTracePayload(traceCacheFile, traceStore, now);
                DashboardOverlayTileRenderer.renderTraceTiles(traceCacheFile, mapConfigFile, tileRoot);
                TimeUnit.MILLISECONDS.sleep(WRITE_INTERVAL.toMillis());
            }
        } finally {
            spark.stop();
        }
    }

    private static void rebuildRecentTraceStore(
            SparkSession spark,
            Path parquetDir,
            RouteMapper routeMapper,
            TraceStore traceStore,
            Instant now
    ) throws Exception {
        List<IncrementalParquetSupport.ParquetFileInfo> recentFiles = IncrementalParquetSupport.listRecentParquetFiles(
                parquetDir,
                now.minus(TRACE_WINDOW).minus(FILE_LOOKBACK_SLACK)
        );
        if (recentFiles.isEmpty()) {
            return;
        }

        Dataset<Row> rows = spark.read()
                .parquet(recentFiles.stream().map(file -> file.path).toArray(String[]::new))
                .select("plate", "internalRouteId", "realRouteNumber", "latitude", "longitude", "eventTime")
                .filter(col("eventTime").isNotNull())
                .filter(col("plate").isNotNull())
                .filter(col("latitude").isNotNull())
                .filter(col("longitude").isNotNull())
                .filter(col("eventTime").geq(java.sql.Timestamp.from(now.minus(TRACE_WINDOW))))
                .orderBy(col("plate").asc(), col("eventTime").asc());

        for (Row row : rows.collectAsList()) {
            String plate = trimToNull(row.getString(0));
            String internalRouteId = trimToNull(row.getString(1));
            String routeNumber = routeMapper.getDisplayRouteNumber(internalRouteId, trimToNull(row.getString(2)));
            double latitude = row.getDouble(3);
            double longitude = row.getDouble(4);
            java.sql.Timestamp eventTime = row.getTimestamp(5);
            if (plate == null || eventTime == null) {
                continue;
            }
            Instant eventInstant = eventTime.toInstant();
            traceStore.addPoint(
                    plate,
                    routeNumber,
                    latitude,
                    longitude,
                    eventInstant,
                    eventInstant
            );
        }
    }

    private static void writeTracePayload(Path traceCacheFile, TraceStore traceStore, Instant now) throws IOException {
        Map<String, Object> payload = traceStore.toPayload(now);
        writeJsonAtomic(traceCacheFile, payload);
    }

    private static Map<String, Object> emptyPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("windowMinutes", TRACE_WINDOW.toMinutes());
        payload.put("maxPointsPerBus", MAX_POINTS_PER_BUS);
        payload.put("buses", List.of());
        return payload;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static void writeJsonAtomic(Path target, Object payload) throws IOException {
        Path tempFile = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static final class TraceStore {
        private final Map<String, BusTrace> buses = new TreeMap<>();
        private final int maxPoints;
        private final Duration window;

        private TraceStore(int maxPoints, Duration window) {
            this.maxPoints = maxPoints;
            this.window = window;
        }

        private boolean addPoint(
                String plate,
                String routeNumber,
                double latitude,
                double longitude,
                Instant timestamp,
                Instant observedAt
        ) {
            BusTrace trace = buses.computeIfAbsent(plate, BusTrace::new);
            trace.routeNumber = routeNumber == null ? trace.routeNumber : routeNumber;

            TracePoint lastPoint = trace.points.peekLast();
            if (lastPoint != null
                    && lastPoint.timestamp.equals(timestamp)
                    && Double.compare(lastPoint.latitude, latitude) == 0
                    && Double.compare(lastPoint.longitude, longitude) == 0) {
                trace.lastSeen = timestamp.isAfter(trace.lastSeen) ? timestamp : trace.lastSeen;
                trace.lastObservedAt = trace.lastObservedAt == null || observedAt.isAfter(trace.lastObservedAt)
                        ? observedAt
                        : trace.lastObservedAt;
                return false;
            }

            trace.points.addLast(new TracePoint(latitude, longitude, timestamp, observedAt));
            while (trace.points.size() > maxPoints) {
                trace.points.removeFirst();
            }
            trace.lastSeen = trace.lastSeen == null || timestamp.isAfter(trace.lastSeen) ? timestamp : trace.lastSeen;
            trace.lastObservedAt = trace.lastObservedAt == null || observedAt.isAfter(trace.lastObservedAt)
                    ? observedAt
                    : trace.lastObservedAt;
            return true;
        }

        private boolean prune(Instant now) {
            Instant cutoff = now.minus(window);
            boolean changed = false;
            List<String> emptyPlates = new ArrayList<>();

            for (BusTrace trace : buses.values()) {
                while (!trace.points.isEmpty() && trace.points.peekFirst().timestamp.isBefore(cutoff)) {
                    trace.points.removeFirst();
                    changed = true;
                }

                if (trace.points.isEmpty() || trace.lastSeen == null || trace.lastSeen.isBefore(cutoff)) {
                    emptyPlates.add(trace.plate);
                    changed = true;
                } else {
                    trace.lastSeen = trace.points.peekLast().timestamp;
                    trace.lastObservedAt = trace.points.peekLast().observedAt;
                }
            }

            for (String plate : emptyPlates) {
                buses.remove(plate);
            }
            return changed;
        }

        private Map<String, Object> toPayload(Instant now) {
            List<Map<String, Object>> items = buses.values().stream()
                    .filter(trace -> !trace.points.isEmpty())
                    .sorted(Comparator.comparing((BusTrace trace) -> trace.lastSeen).reversed().thenComparing(trace -> trace.plate))
                    .map(BusTrace::toPayload)
                    .collect(Collectors.toList());

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("updatedAt", now.atOffset(ZoneOffset.UTC).toString());
            payload.put("status", items.isEmpty() ? "empty" : "ok");
            if (items.isEmpty()) {
                payload.put("message", "No active buses in the last 10 minutes");
            }
            payload.put("windowMinutes", window.toMinutes());
            payload.put("maxPointsPerBus", maxPoints);
            payload.put("busCount", items.size());
            payload.put("buses", items);
            return payload;
        }
    }

    private static final class BusTrace {
        private final String plate;
        private final Deque<TracePoint> points = new ArrayDeque<>();
        private String routeNumber;
        private Instant lastSeen;
        private Instant lastObservedAt;

        private BusTrace(String plate) {
            this.plate = plate;
        }

        private Map<String, Object> toPayload() {
            List<Map<String, Object>> pointItems = new ArrayList<>(points.size());
            int index = 0;
            for (TracePoint point : points) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("latitude", point.latitude);
                item.put("longitude", point.longitude);
                item.put("timestamp", point.timestamp.toString());
                item.put("ageIndex", index++);
                pointItems.add(item);
            }

            TracePoint latest = points.peekLast();

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("plate", plate);
            payload.put("routeNumber", routeNumber);
            payload.put("lastSeen", lastSeen == null ? null : lastSeen.toString());
            payload.put("lastObservedAt", lastObservedAt == null ? null : lastObservedAt.toString());
            payload.put("latestLatitude", latest == null ? null : latest.latitude);
            payload.put("latestLongitude", latest == null ? null : latest.longitude);
            payload.put("pointCount", pointItems.size());
            payload.put("points", pointItems);
            return payload;
        }
    }

    private static final class TracePoint {
        private final double latitude;
        private final double longitude;
        private final Instant timestamp;
        private final Instant observedAt;

        private TracePoint(double latitude, double longitude, Instant timestamp, Instant observedAt) {
            this.latitude = latitude;
            this.longitude = longitude;
            this.timestamp = Objects.requireNonNull(timestamp);
            this.observedAt = Objects.requireNonNull(observedAt);
        }
    }
}
