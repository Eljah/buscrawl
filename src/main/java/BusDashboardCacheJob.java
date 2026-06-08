import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.RandomAccessFile;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.window;

public class BusDashboardCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Duration STATS_WINDOW = Duration.ofHours(24);
    private static final Duration FILE_LOOKBACK_SLACK = Duration.ofMinutes(30);
    private static final ZoneId CITY_ZONE = ZoneId.of(
            System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow")
    );
    private static final DateTimeFormatter BUCKET_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {
        Path parquetDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path statsCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_CACHE_FILE",
                "./var/bus/dashboard-cache/stats.json"
        ));
        Path routeMovementCacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_ROUTE_CACHE_FILE",
                "./var/bus/dashboard-cache/route-last-movement.json"
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_STATE_FILE",
                statsCacheFile.resolveSibling("cache-state.json").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_SPARK_LOCAL_DIR",
                "./var/bus/dashboard-spark-temp"
        ));
        Path trafficBehaviorDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_DASHBOARD_SPARK_MASTER", "local[2]");

        Files.createDirectories(statsCacheFile.getParent());
        Files.createDirectories(routeMovementCacheFile.getParent());
        Files.createDirectories(stateFile.getParent());
        Files.createDirectories(sparkLocalDir);

        if (!Files.exists(parquetDir)) {
            writeJsonAtomic(statsCacheFile, emptyStatsPayload("Parquet directory not found: " + parquetDir));
            writeJsonAtomic(routeMovementCacheFile, emptyRoutePayload("Parquet directory not found: " + parquetDir));
            writeJsonAtomic(stateFile, emptyState());
            return;
        }

        Instant now = Instant.now();
        LocalDate today = now.atZone(CITY_ZONE).toLocalDate();
        LocalDate yesterday = today.minusDays(1);
        Instant statsStart = now.minus(STATS_WINDOW);
        Instant previousDayStart = yesterday.atStartOfDay(CITY_ZONE).toInstant();
        Instant nextDayStart = today.plusDays(1).atStartOfDay(CITY_ZONE).toInstant();
        Instant fileCutoff = minInstant(statsStart, previousDayStart).minus(FILE_LOOKBACK_SLACK);

        List<ParquetFileInfo> candidateFiles = listRecentParquetFiles(parquetDir, fileCutoff);
        DashboardState state = loadState(stateFile, fileCutoff, statsCacheFile, routeMovementCacheFile);
        List<ParquetFileInfo> newFiles = selectNewFiles(candidateFiles, state.lastProcessedModifiedAt);

        Map<Instant, Long> statsBuckets = loadStatsBuckets(statsCacheFile, statsStart);
        RouteCache routeCache = loadRouteCache(routeMovementCacheFile, today, yesterday);
        RouteMapper routeMapper = new RouteMapper(null);

        System.out.printf(
                "BusDashboardCacheJob: %d candidate parquet files, %d new files since %s%n",
                candidateFiles.size(),
                newFiles.size(),
                fileCutoff
        );

        DerivedStats derivedStats = DerivedStats.empty();
        if (!newFiles.isEmpty() || Files.exists(trafficBehaviorDir)) {
            SparkSession spark = SparkSession.builder()
                    .appName("BusDashboardCacheJob")
                    .master(sparkMaster)
                    .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                    .config("spark.driver.memory", System.getenv().getOrDefault("BUS_DASHBOARD_SPARK_DRIVER_MEMORY", "2g"))
                    .config("spark.executor.memory", System.getenv().getOrDefault("BUS_DASHBOARD_SPARK_EXECUTOR_MEMORY", "2g"))
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            try {
                if (!newFiles.isEmpty()) {
                    Dataset<Row> newBusData = spark.read()
                            .parquet(newFiles.stream().map(file -> file.path).toArray(String[]::new))
                            .select("internalRouteId", "realRouteNumber", "eventTime", "speed")
                            .filter(col("eventTime").isNotNull());

                    mergeStatsBuckets(statsBuckets, newBusData, statsStart, now);
                    mergeRouteCache(routeMapper, routeCache, newBusData, previousDayStart, today.atStartOfDay(CITY_ZONE).toInstant(), nextDayStart);
                }
                derivedStats = computeDerivedStats(spark, trafficBehaviorDir, statsStart, now);
            } finally {
                spark.stop();
            }
        }

        pruneStatsBuckets(statsBuckets, statsStart);

        Map<String, Object> statsPayload = buildStatsPayload(statsBuckets, derivedStats, statsStart, now);
        Map<String, Object> routePayload = buildRoutePayload(routeMapper, routeCache, today, yesterday);

        state.updatedAt = now.atOffset(ZoneOffset.UTC).toString();
        state.lastProcessedModifiedAt = candidateFiles.isEmpty()
                ? state.lastProcessedModifiedAt
                : candidateFiles.get(candidateFiles.size() - 1).modifiedAt;

        writeJsonAtomic(statsCacheFile, statsPayload);
        writeJsonAtomic(routeMovementCacheFile, routePayload);
        writeJsonAtomic(stateFile, state);
    }

    private static void mergeStatsBuckets(Map<Instant, Long> statsBuckets, Dataset<Row> newBusData, Instant statsStart, Instant rangeEnd) {
        Timestamp rangeStartTs = Timestamp.from(statsStart);
        Timestamp rangeEndTs = Timestamp.from(rangeEnd);

        List<Row> rows = newBusData
                .filter(col("eventTime").geq(rangeStartTs).and(col("eventTime").leq(rangeEndTs)))
                .withColumn("bucket_time", window(col("eventTime"), "5 minutes").getField("start"))
                .groupBy("bucket_time")
                .count()
                .collectAsList();

        for (Row row : rows) {
            Timestamp bucketTime = row.getTimestamp(0);
            long points = row.getLong(1);
            statsBuckets.merge(bucketTime.toInstant(), points, Long::sum);
        }
    }

    private static void mergeRouteCache(RouteMapper routeMapper, RouteCache routeCache, Dataset<Row> newBusData, Instant previousDayStart, Instant currentDayStart, Instant nextDayStart) {
        Dataset<Row> movingData = newBusData
                .filter(col("speed").gt(0))
                .filter(col("eventTime").geq(Timestamp.from(previousDayStart))
                        .and(col("eventTime").lt(Timestamp.from(nextDayStart))));

        Map<String, Instant> currentDayMap = collectMaxEventTimeByRoute(
                routeMapper,
                movingData.filter(col("eventTime").geq(Timestamp.from(currentDayStart)))
        );
        Map<String, Instant> previousDayMap = collectMaxEventTimeByRoute(
                routeMapper,
                movingData.filter(col("eventTime").geq(Timestamp.from(previousDayStart))
                        .and(col("eventTime").lt(Timestamp.from(currentDayStart))))
        );

        currentDayMap.forEach((routeNumber, instant) -> routeCache.currentDay.merge(routeNumber, instant, BusDashboardCacheJob::maxInstant));
        previousDayMap.forEach((routeNumber, instant) -> routeCache.previousDay.merge(routeNumber, instant, BusDashboardCacheJob::maxInstant));
    }

    private static DerivedStats computeDerivedStats(SparkSession spark, Path trafficBehaviorDir, Instant rangeStart, Instant rangeEnd) {
        if (!Files.exists(trafficBehaviorDir)) {
            return DerivedStats.empty();
        }

        Map<String, List<Map<String, Object>>> series = new LinkedHashMap<>();
        series.put("segmentTrips", collectDerivedSeries(
                spark,
                trafficBehaviorDir.resolve("segment-trips"),
                "endEnteredStopAt",
                rangeStart,
                rangeEnd,
                null
        ));
        series.put("overtakes", collectDerivedSeries(
                spark,
                trafficBehaviorDir.resolve("overtake-events"),
                "overtakeAt",
                rangeStart,
                rangeEnd,
                null
        ));
        series.put("physicalOvertakes", collectDerivedSeries(
                spark,
                trafficBehaviorDir.resolve("physical-overtake-events"),
                "overtakeAt",
                rangeStart,
                rangeEnd,
                null
        ));
        series.put("dwellEvents", collectDerivedSeries(
                spark,
                trafficBehaviorDir.resolve("dwell-events"),
                "enteredStopAt",
                rangeStart,
                rangeEnd,
                null
        ));
        series.put("dwellEventsOver60", collectDerivedSeries(
                spark,
                trafficBehaviorDir.resolve("dwell-events"),
                "enteredStopAt",
                rangeStart,
                rangeEnd,
                "dwellTimeSeconds > 60"
        ));

        Map<String, Long> totals = new LinkedHashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : series.entrySet()) {
            long total = 0L;
            for (Map<String, Object> item : entry.getValue()) {
                Object points = item.get("points");
                if (points instanceof Number) {
                    total += ((Number) points).longValue();
                }
            }
            totals.put(entry.getKey(), total);
        }

        return new DerivedStats(series, totals);
    }

    private static List<Map<String, Object>> collectDerivedSeries(
            SparkSession spark,
            Path parquetDir,
            String timestampColumn,
            Instant rangeStart,
            Instant rangeEnd,
            String filterExpression
    ) {
        if (!hasReadableParquetFiles(parquetDir)) {
            return List.of();
        }

        Timestamp rangeStartTs = Timestamp.from(rangeStart);
        Timestamp rangeEndTs = Timestamp.from(rangeEnd);
        Dataset<Row> filtered = spark.read()
                .parquet(parquetDir.toAbsolutePath().toString())
                .filter(col(timestampColumn).isNotNull())
                .filter(col(timestampColumn).geq(rangeStartTs).and(col(timestampColumn).leq(rangeEndTs)));
        if (filterExpression != null && !filterExpression.isBlank()) {
            filtered = filtered.filter(expr(filterExpression));
        }
        List<Row> rows = filtered
                .withColumn("bucket_time", window(col(timestampColumn), "5 minutes").getField("start"))
                .groupBy("bucket_time")
                .count()
                .sort("bucket_time")
                .collectAsList();

        List<Map<String, Object>> result = new ArrayList<>();
        for (Row row : rows) {
            Timestamp bucketTime = row.getTimestamp(0);
            long points = row.getLong(1);

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("label", BUCKET_LABEL.format(bucketTime.toInstant().atZone(CITY_ZONE)));
            item.put("points", points);
            result.add(item);
        }
        return result;
    }

    private static boolean hasReadableParquetFiles(Path parquetDir) {
        if (!Files.exists(parquetDir)) {
            return false;
        }
        try (Stream<Path> files = Files.walk(parquetDir, FileVisitOption.FOLLOW_LINKS)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(BusDashboardCacheJob::isStableParquetPath)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .anyMatch(BusDashboardCacheJob::isReadableParquetFile);
        } catch (IOException e) {
            return false;
        }
    }

    private static Map<String, Object> buildStatsPayload(Map<Instant, Long> statsBuckets, DerivedStats derivedStats, Instant rangeStart, Instant rangeEnd) {
        List<Map<String, Object>> series = new ArrayList<>();
        long totalPoints = 0;
        Instant bucketStart = floorToBucket(rangeStart);
        Instant bucketEnd = floorToBucket(rangeEnd);
        Instant firstDataBucket = null;
        Instant lastDataBucket = null;

        TreeMap<Instant, Long> sortedBuckets = new TreeMap<>(statsBuckets);
        for (Instant bucket = bucketStart; !bucket.isAfter(bucketEnd); bucket = bucket.plus(Duration.ofMinutes(5))) {
            long points = sortedBuckets.getOrDefault(bucket, 0L);
            totalPoints += points;
            if (points > 0) {
                if (firstDataBucket == null) {
                    firstDataBucket = bucket;
                }
                lastDataBucket = bucket;
            }

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("label", BUCKET_LABEL.format(bucket.atZone(CITY_ZONE)));
            item.put("points", points);
            series.add(item);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", series.isEmpty() ? "empty" : "ok");
        if (series.isEmpty()) {
            payload.put("message", "No points found in the last 24 hours");
        }
        payload.put("totalPoints", totalPoints);
        payload.put("bucket", "5 minutes");
        payload.put("downsampled", false);
        payload.put("rangeStart", rangeStart.toString());
        payload.put("rangeEnd", rangeEnd.toString());
        payload.put("bucketRangeStart", bucketStart.toString());
        payload.put("bucketRangeEnd", bucketEnd.toString());
        payload.put("dataStart", firstDataBucket == null ? null : firstDataBucket.toString());
        payload.put("dataEnd", lastDataBucket == null ? null : lastDataBucket.toString());
        payload.put("series", series);
        payload.put("derivedSeries", derivedStats.series);
        payload.put("derivedTotals", derivedStats.totals);
        return payload;
    }

    private static Instant floorToBucket(Instant instant) {
        long bucketSeconds = Duration.ofMinutes(5).toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(instant.getEpochSecond(), bucketSeconds) * bucketSeconds);
    }

    private static Map<String, Object> buildRoutePayload(RouteMapper routeMapper, RouteCache routeCache, LocalDate today, LocalDate yesterday) {
        List<String> routeNumbers = routeMapper.getAllDisplayRouteNumbers();
        LinkedHashSet<String> allRoutes = new LinkedHashSet<>(routeNumbers);
        List<String> extraRoutes = new ArrayList<>();
        extraRoutes.addAll(routeCache.currentDay.keySet());
        extraRoutes.addAll(routeCache.previousDay.keySet());
        extraRoutes.sort(Comparator.comparing(BusDashboardCacheJob::routeSortKey));
        allRoutes.addAll(extraRoutes);

        List<Map<String, Object>> routes = new ArrayList<>();
        for (String routeNumber : allRoutes) {
            Instant current = routeCache.currentDay.get(routeNumber);
            Instant previous = routeCache.previousDay.get(routeNumber);

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("routeNumber", routeNumber);
            row.put("currentDayLastMovement", current == null ? null : current.toString());
            row.put("previousDayLastMovement", previous == null ? null : previous.toString());
            row.put("displayLastMovement", current != null ? current.toString() : previous == null ? null : previous.toString());
            row.put("sourceDay", current != null ? "current" : previous != null ? "previous" : "none");
            routes.add(row);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "ok");
        payload.put("today", today.toString());
        payload.put("yesterday", yesterday.toString());
        payload.put("routes", routes);
        return payload;
    }

    private static Map<String, Instant> collectMaxEventTimeByRoute(RouteMapper routeMapper, Dataset<Row> dataset) {
        Map<String, Instant> result = new LinkedHashMap<>();
        List<Row> rows = dataset.groupBy("internalRouteId", "realRouteNumber")
                .agg(max("eventTime").alias("last_movement"))
                .collectAsList();

        for (Row row : rows) {
            String internalRouteId = row.getString(0);
            String storedRouteNumber = row.getString(1);
            Timestamp lastMovement = row.getTimestamp(2);
            String routeNumber = routeMapper.getDisplayRouteNumber(internalRouteId, storedRouteNumber);
            if (routeNumber != null && lastMovement != null) {
                result.merge(routeNumber, lastMovement.toInstant(), BusDashboardCacheJob::maxInstant);
            }
        }
        return result;
    }

    private static Map<Instant, Long> loadStatsBuckets(Path statsCacheFile, Instant statsStart) throws IOException {
        Map<Instant, Long> buckets = new TreeMap<>();
        if (!Files.exists(statsCacheFile)) {
            return buckets;
        }

        Map<String, Object> payload = readJsonMap(statsCacheFile);
        Object seriesObject = payload.get("series");
        if (!(seriesObject instanceof Collection<?>)) {
            return buckets;
        }

        for (Object itemObject : (Collection<?>) seriesObject) {
            if (!(itemObject instanceof Map<?, ?>)) {
                continue;
            }
            Map<?, ?> item = (Map<?, ?>) itemObject;
            Object labelObject = item.get("label");
            Object pointsObject = item.get("points");
            if (labelObject == null || pointsObject == null) {
                continue;
            }

            Instant bucketStart = parseBucketLabel(String.valueOf(labelObject));
            if (bucketStart == null || bucketStart.isBefore(statsStart)) {
                continue;
            }

            buckets.put(bucketStart, ((Number) pointsObject).longValue());
        }
        return buckets;
    }

    private static RouteCache loadRouteCache(Path routeCacheFile, LocalDate today, LocalDate yesterday) throws IOException {
        RouteCache routeCache = new RouteCache();
        routeCache.today = today;
        routeCache.yesterday = yesterday;

        if (!Files.exists(routeCacheFile)) {
            return routeCache;
        }

        Map<String, Object> payload = readJsonMap(routeCacheFile);
        LocalDate cachedToday = parseLocalDate(payload.get("today"));
        LocalDate cachedYesterday = parseLocalDate(payload.get("yesterday"));

        Map<String, Instant> cachedCurrent = new LinkedHashMap<>();
        Map<String, Instant> cachedPrevious = new LinkedHashMap<>();

        Object routesObject = payload.get("routes");
        if (routesObject instanceof Collection<?>) {
            for (Object routeObject : (Collection<?>) routesObject) {
                if (!(routeObject instanceof Map<?, ?>)) {
                    continue;
                }
                Map<?, ?> route = (Map<?, ?>) routeObject;
                Object routeNumberObject = route.get("routeNumber");
                if (routeNumberObject == null) {
                    continue;
                }
                String routeNumber = String.valueOf(routeNumberObject);
                Instant current = parseInstant(route.get("currentDayLastMovement"));
                Instant previous = parseInstant(route.get("previousDayLastMovement"));
                if (current != null) {
                    cachedCurrent.put(routeNumber, current);
                }
                if (previous != null) {
                    cachedPrevious.put(routeNumber, previous);
                }
            }
        }

        if (today.equals(cachedToday) && yesterday.equals(cachedYesterday)) {
            routeCache.currentDay.putAll(cachedCurrent);
            routeCache.previousDay.putAll(cachedPrevious);
            return routeCache;
        }

        if (yesterday.equals(cachedToday)) {
            routeCache.previousDay.putAll(cachedCurrent);
        }

        return routeCache;
    }

    private static DashboardState loadState(
            Path stateFile,
            Instant fileCutoff,
            Path statsCacheFile,
            Path routeCacheFile
    ) throws IOException {
        DashboardState state;
        if (Files.exists(stateFile)) {
            state = MAPPER.readValue(stateFile.toFile(), DashboardState.class);
        } else {
            state = seedStateFromExistingCaches(statsCacheFile, routeCacheFile);
        }
        if (state.lastProcessedModifiedAt != null) {
            Instant lastProcessed = parseInstant(state.lastProcessedModifiedAt);
            if (lastProcessed != null && lastProcessed.isBefore(fileCutoff)) {
                state.lastProcessedModifiedAt = fileCutoff.toString();
            }
        }

        return state;
    }

    private static DashboardState seedStateFromExistingCaches(
            Path statsCacheFile,
            Path routeCacheFile
    ) throws IOException {
        DashboardState state = emptyState();
        Instant cacheBaseline = latestExistingCacheInstant(statsCacheFile, routeCacheFile);
        if (cacheBaseline == null) {
            return state;
        }

        state.updatedAt = cacheBaseline.atOffset(ZoneOffset.UTC).toString();
        state.lastProcessedModifiedAt = cacheBaseline.toString();
        return state;
    }

    private static Instant latestExistingCacheInstant(Path statsCacheFile, Path routeCacheFile) throws IOException {
        Instant latest = null;
        if (Files.exists(statsCacheFile)) {
            latest = Files.getLastModifiedTime(statsCacheFile).toInstant();
        }
        if (Files.exists(routeCacheFile)) {
            Instant routeInstant = Files.getLastModifiedTime(routeCacheFile).toInstant();
            latest = latest == null || routeInstant.isAfter(latest) ? routeInstant : latest;
        }
        return latest;
    }

    private static List<ParquetFileInfo> selectNewFiles(List<ParquetFileInfo> candidateFiles, String lastProcessedModifiedAtText) {
        Instant lastProcessedModifiedAt = parseInstant(lastProcessedModifiedAtText);
        List<ParquetFileInfo> newFiles = new ArrayList<>();
        for (ParquetFileInfo file : candidateFiles) {
            Instant fileModifiedAt = parseInstant(file.modifiedAt);
            if (fileModifiedAt != null && (lastProcessedModifiedAt == null || fileModifiedAt.isAfter(lastProcessedModifiedAt))) {
                newFiles.add(file);
            }
        }
        return newFiles;
    }

    private static List<ParquetFileInfo> listRecentParquetFiles(Path parquetDir, Instant modifiedSince) throws IOException {
        FileTime modifiedSinceTime = FileTime.from(modifiedSince);
        try (Stream<Path> files = Files.walk(parquetDir, FileVisitOption.FOLLOW_LINKS)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(BusDashboardCacheJob::isStableParquetPath)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .map(path -> toParquetFileInfo(path, modifiedSinceTime))
                    .filter(file -> file != null)
                    .sorted(Comparator.comparing((ParquetFileInfo file) -> parseInstant(file.modifiedAt)).thenComparing(file -> file.path))
                    .collect(Collectors.toList());
        }
    }

    private static ParquetFileInfo toParquetFileInfo(Path path, FileTime modifiedSinceTime) {
        try {
            FileTime modifiedAt = Files.getLastModifiedTime(path);
            if (!isReadableParquetFile(path) || modifiedAt.compareTo(modifiedSinceTime) < 0) {
                return null;
            }
            ParquetFileInfo file = new ParquetFileInfo();
            file.path = path.toAbsolutePath().toString();
            file.modifiedAt = modifiedAt.toInstant().toString();
            return file;
        } catch (IOException e) {
            System.err.println("Skipping parquet file " + path + ": " + e.getMessage());
            return null;
        }
    }

    private static boolean isStableParquetPath(Path path) {
        for (Path part : path) {
            String name = part.toString();
            if (name.startsWith(".") || "_temporary".equals(name)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isReadableParquetFile(Path path) {
        try {
            long size = Files.size(path);
            if (size < 8L) {
                return false;
            }

            byte[] header = new byte[4];
            byte[] footer = new byte[4];
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
                file.readFully(header);
                file.seek(size - 4L);
                file.readFully(footer);
            }

            return isParquetMagic(header) && isParquetMagic(footer);
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean isParquetMagic(byte[] bytes) {
        return bytes.length == 4
                && bytes[0] == 'P'
                && bytes[1] == 'A'
                && bytes[2] == 'R'
                && bytes[3] == '1';
    }

    private static void pruneStatsBuckets(Map<Instant, Long> statsBuckets, Instant statsStart) {
        statsBuckets.entrySet().removeIf(entry -> entry.getKey().isBefore(statsStart));
    }

    private static Map<String, Object> emptyStatsPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("totalPoints", 0);
        payload.put("bucket", "5 minutes");
        payload.put("downsampled", false);
        payload.put("rangeStart", null);
        payload.put("rangeEnd", null);
        payload.put("series", List.of());
        payload.put("derivedSeries", Map.of());
        payload.put("derivedTotals", Map.of());
        return payload;
    }

    private static Map<String, Object> emptyRoutePayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("today", null);
        payload.put("yesterday", null);
        payload.put("routes", List.of());
        return payload;
    }

    private static DashboardState emptyState() {
        DashboardState state = new DashboardState();
        state.updatedAt = Instant.now().atOffset(ZoneOffset.UTC).toString();
        state.lastProcessedModifiedAt = null;
        return state;
    }

    private static Instant parseBucketLabel(String label) {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(label, BUCKET_LABEL);
            return localDateTime.atZone(CITY_ZONE).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Instant parseInstant(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(String.valueOf(value));
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static LocalDate parseLocalDate(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return LocalDate.parse(String.valueOf(value));
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Map<String, Object> readJsonMap(Path path) throws IOException {
        return MAPPER.readValue(path.toFile(), MAP_TYPE);
    }

    private static String routeSortKey(String routeNumber) {
        String digits = routeNumber.replaceAll("[^0-9]", "");
        String suffix = routeNumber.replaceAll("[0-9]", "");
        String numberPart = digits.isEmpty() ? "9999" : String.format("%04d", Integer.parseInt(digits));
        return numberPart + ":" + suffix + ":" + routeNumber;
    }

    private static Instant minInstant(Instant left, Instant right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static Instant maxInstant(Instant left, Instant right) {
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws IOException {
        Files.createDirectories(targetFile.getParent());
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static final class DashboardState {
        public String updatedAt;
        public String lastProcessedModifiedAt;
    }

    public static final class ParquetFileInfo {
        public String path;
        public String modifiedAt;
    }

    private static final class DerivedStats {
        private final Map<String, List<Map<String, Object>>> series;
        private final Map<String, Long> totals;

        private DerivedStats(Map<String, List<Map<String, Object>>> series, Map<String, Long> totals) {
            this.series = series;
            this.totals = totals;
        }

        private static DerivedStats empty() {
            return new DerivedStats(Map.of(), Map.of());
        }
    }

    private static final class RouteCache {
        private LocalDate today;
        private LocalDate yesterday;
        private final Map<String, Instant> currentDay = new LinkedHashMap<>();
        private final Map<String, Instant> previousDay = new LinkedHashMap<>();
    }
}
