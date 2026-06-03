import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BusActiveTraceCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TRACE_WINDOW = Duration.ofMinutes(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_WINDOW_MINUTES", "10"))
    );
    private static final Duration BOOTSTRAP_LOOKBACK = Duration.ofMinutes(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_BOOTSTRAP_LOOKBACK_MINUTES", "30"))
    );
    private static final Duration WRITE_INTERVAL = Duration.ofSeconds(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_WRITE_INTERVAL_SECONDS", "5"))
    );
    private static final Duration FILE_SETTLE_DELAY = Duration.ofSeconds(
            Long.parseLong(System.getenv().getOrDefault("BUS_TRACE_FILE_SETTLE_SECONDS", "3"))
    );
    private static final int MAX_POINTS_PER_BUS = Integer.parseInt(
            System.getenv().getOrDefault("BUS_TRACE_POINTS_LIMIT", "100")
    );

    public static void main(String[] args) throws Exception {
        Path parquetDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path traceCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_TRACE_CACHE_FILE",
                "./var/bus/dashboard-cache/bus-traces.json"
        ));

        Files.createDirectories(traceCacheFile.getParent());

        if (!Files.isDirectory(parquetDir)) {
            writeJsonAtomic(traceCacheFile, emptyPayload("Parquet directory not found: " + parquetDir));
            return;
        }

        Class.forName("org.duckdb.DuckDBDriver");

        TraceStore traceStore = new TraceStore(MAX_POINTS_PER_BUS, TRACE_WINDOW);
        Map<Path, Long> processedFiles = new LinkedHashMap<>();
        Set<Path> pendingFiles = new LinkedHashSet<>();

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             WatchService watchService = FileSystems.getDefault().newWatchService()) {
            parquetDir.register(
                    watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY
            );

            bootstrapRecentFiles(connection, parquetDir, traceStore, processedFiles);
            traceStore.prune(Instant.now());
            writeTracePayload(traceCacheFile, traceStore, Instant.now());

            Instant lastWrite = Instant.now();
            while (true) {
                WatchKey watchKey = watchService.poll(2, TimeUnit.SECONDS);
                if (watchKey != null) {
                    for (WatchEvent<?> event : watchKey.pollEvents()) {
                        if (!(event.context() instanceof Path)) {
                            continue;
                        }
                        Path relative = (Path) event.context();
                        if (!relative.getFileName().toString().endsWith(".parquet")) {
                            continue;
                        }
                        pendingFiles.add(parquetDir.resolve(relative));
                    }
                    watchKey.reset();
                }

                boolean changed = processPendingFiles(connection, traceStore, processedFiles, pendingFiles);
                Instant now = Instant.now();
                changed = traceStore.prune(now) || changed;
                if (changed || Duration.between(lastWrite, now).compareTo(WRITE_INTERVAL) >= 0) {
                    writeTracePayload(traceCacheFile, traceStore, now);
                    lastWrite = now;
                }
            }
        }
    }

    private static void bootstrapRecentFiles(
            Connection connection,
            Path parquetDir,
            TraceStore traceStore,
            Map<Path, Long> processedFiles
    ) throws Exception {
        Instant cutoff = Instant.now().minus(BOOTSTRAP_LOOKBACK);
        List<Path> files;
        try (Stream<Path> stream = Files.list(parquetDir)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .filter(path -> isUsefulParquet(path, cutoff))
                    .sorted(Comparator.comparing(BusActiveTraceCacheJob::safeFileTime))
                    .collect(Collectors.toList());
        }

        for (Path file : files) {
            processSingleFile(connection, traceStore, processedFiles, file);
        }
    }

    private static boolean processPendingFiles(
            Connection connection,
            TraceStore traceStore,
            Map<Path, Long> processedFiles,
            Set<Path> pendingFiles
    ) throws Exception {
        if (pendingFiles.isEmpty()) {
            return false;
        }

        boolean changed = false;
        List<Path> readyFiles = new ArrayList<>();
        Instant settledBefore = Instant.now().minus(FILE_SETTLE_DELAY);
        for (Path file : new ArrayList<>(pendingFiles)) {
            if (!Files.exists(file) || !Files.isRegularFile(file)) {
                pendingFiles.remove(file);
                continue;
            }

            FileTime modifiedAt = safeFileTime(file);
            if (modifiedAt.toInstant().isAfter(settledBefore)) {
                continue;
            }

            pendingFiles.remove(file);
            readyFiles.add(file);
        }

        readyFiles.sort(Comparator.comparing(BusActiveTraceCacheJob::safeFileTime));
        for (Path file : readyFiles) {
            changed = processSingleFile(connection, traceStore, processedFiles, file) || changed;
        }
        return changed;
    }

    private static boolean processSingleFile(
            Connection connection,
            TraceStore traceStore,
            Map<Path, Long> processedFiles,
            Path file
    ) throws Exception {
        if (!Files.exists(file) || Files.size(file) <= 4L || !file.getFileName().toString().endsWith(".parquet")) {
            return false;
        }

        long modifiedAt = safeFileTime(file).toMillis();
        Long previous = processedFiles.get(file);
        if (previous != null && previous >= modifiedAt) {
            return false;
        }

        String sql = "SELECT plate, realRouteNumber, latitude, longitude, eventTime " +
                "FROM read_parquet(?) " +
                "WHERE eventTime IS NOT NULL AND plate IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL " +
                "ORDER BY eventTime ASC";

        boolean changed = false;
        Instant observedAt = Instant.ofEpochMilli(modifiedAt);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, file.toAbsolutePath().toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String plate = trimToNull(resultSet.getString(1));
                    String routeNumber = trimToNull(resultSet.getString(2));
                    double latitude = resultSet.getDouble(3);
                    double longitude = resultSet.getDouble(4);
                    Timestamp eventTime = resultSet.getTimestamp(5);
                    if (plate == null || eventTime == null) {
                        continue;
                    }
                    changed = traceStore.addPoint(
                            plate,
                            routeNumber,
                            latitude,
                            longitude,
                            eventTime.toInstant(),
                            observedAt
                    ) || changed;
                }
            }
        }

        processedFiles.put(file, modifiedAt);
        return changed;
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

    private static boolean isUsefulParquet(Path path, Instant cutoff) {
        try {
            return Files.size(path) > 4L && Files.getLastModifiedTime(path).toInstant().compareTo(cutoff) >= 0;
        } catch (IOException e) {
            return false;
        }
    }

    private static FileTime safeFileTime(Path path) {
        try {
            return Files.getLastModifiedTime(path);
        } catch (IOException e) {
            return FileTime.fromMillis(0L);
        }
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
                while (!trace.points.isEmpty() && trace.points.peekFirst().observedAt.isBefore(cutoff)) {
                    trace.points.removeFirst();
                    changed = true;
                }

                if (trace.points.isEmpty() || trace.lastObservedAt == null || trace.lastObservedAt.isBefore(cutoff)) {
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
