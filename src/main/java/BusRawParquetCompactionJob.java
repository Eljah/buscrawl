import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

public class BusRawParquetCompactionJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Path inputDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path outputDir = Path.of(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_DIR", "./var/bus/bus-data-parquet-compacted"));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_STATE_FILE",
                outputDir.resolve("compaction-state.json").toString()
        ));
        int maxFilesPerRun = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_MAX_FILES_PER_RUN", "5000"));
        int minBatchFiles = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_MIN_BATCH_FILES", "100"));
        long minBatchBytes = Long.parseLong(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_MIN_BATCH_BYTES", "67108864"));
        Duration maxOpenLag = Duration.ofMinutes(Long.parseLong(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_MAX_OPEN_LAG_MINUTES", "60")));
        boolean allowSmallTail = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_ALLOW_SMALL_TAIL", "false"));
        int initialLookbackDays = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_INITIAL_LOOKBACK_DAYS", "3"));
        String bootstrapStrategy = System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE", "full-history");
        boolean newestFirst = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_NEWEST_FIRST", "false"));
        long minRawParquetBytes = Long.parseLong(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_MIN_RAW_BYTES", "100"));

        Files.createDirectories(outputDir);
        Files.createDirectories(stateFile.getParent());

        if (!Files.exists(inputDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + inputDir);
        }

        CompactionState state = loadState(stateFile);
        Instant modifiedSince;
        if (state.lastProcessedModifiedAt == null) {
            modifiedSince = "recent-window".equalsIgnoreCase(bootstrapStrategy)
                    ? Instant.now().minus(Duration.ofDays(initialLookbackDays))
                    : Instant.EPOCH;
        } else {
            modifiedSince = IncrementalParquetSupport.parseInstant(state.lastProcessedModifiedAt);
        }
        if (modifiedSince == null) {
            modifiedSince = Instant.now().minus(Duration.ofDays(initialLookbackDays));
        }

        boolean recursiveListing = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_RECURSIVE_LISTING",
                "false"
        ));
        boolean useFindListing = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_USE_FIND_LISTING",
                "true"
        ));
        boolean useManifestListing = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_USE_MANIFEST",
                "true"
        ));
        Path manifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_FILE",
                inputDir.getParent().resolve("bus-data-parquet-manifest.tsv").toString()
        ));
        Path legacyManifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE",
                inputDir.getParent().resolve("bus-data-parquet-legacy-manifest.tsv").toString()
        ));
        System.out.printf(
                "BusRawParquetCompactionJob: listing raw parquet files in %s since %s (manifest=%s, recursive=%s, find=%s)%n",
                inputDir,
                modifiedSince,
                useManifestListing,
                recursiveListing,
                useFindListing
        );
        BusRawParquetManifest.ManifestWindow manifestWindow = null;
        List<IncrementalParquetSupport.ParquetFileInfo> candidateFiles;
        boolean useLegacyManifest = useManifestListing
                && Files.exists(legacyManifestFile)
                && !state.legacyManifestComplete;
        int activeMaxFilesPerRun = useLegacyManifest
                ? Integer.parseInt(System.getenv().getOrDefault(
                        "BUS_COMPACTED_PARQUET_LEGACY_MAX_FILES_PER_RUN",
                        Integer.toString(maxFilesPerRun)
                ))
                : maxFilesPerRun;
        int activeMinBatchFiles = useLegacyManifest
                ? Integer.parseInt(System.getenv().getOrDefault(
                        "BUS_COMPACTED_PARQUET_LEGACY_MIN_BATCH_FILES",
                        Integer.toString(minBatchFiles)
                ))
                : minBatchFiles;
        long activeMinBatchBytes = useLegacyManifest
                ? Long.parseLong(System.getenv().getOrDefault(
                        "BUS_COMPACTED_PARQUET_LEGACY_MIN_BATCH_BYTES",
                        Long.toString(minBatchBytes)
                ))
                : minBatchBytes;
        Duration activeMaxOpenLag = useLegacyManifest
                ? Duration.ofMinutes(Long.parseLong(System.getenv().getOrDefault(
                        "BUS_COMPACTED_PARQUET_LEGACY_MAX_OPEN_LAG_MINUTES",
                        Long.toString(maxOpenLag.toMinutes())
                )))
                : maxOpenLag;
        boolean activeAllowSmallTail = useLegacyManifest
                ? Boolean.parseBoolean(System.getenv().getOrDefault(
                        "BUS_COMPACTED_PARQUET_LEGACY_ALLOW_SMALL_TAIL",
                        Boolean.toString(allowSmallTail)
                ))
                : allowSmallTail;
        String inputMode = useLegacyManifest ? "legacy" : "current";
        System.out.printf(
                "BusRawParquetCompactionJob: inputMode=%s maxFilesPerRun=%d minBatchFiles=%d minBatchBytes=%d maxOpenLag=%s allowSmallTail=%s%n",
                inputMode,
                activeMaxFilesPerRun,
                activeMinBatchFiles,
                activeMinBatchBytes,
                activeMaxOpenLag,
                activeAllowSmallTail
        );
        if (useLegacyManifest) {
            int manifestWindowRecords = Integer.parseInt(System.getenv().getOrDefault(
                    "BUS_COMPACTED_PARQUET_MANIFEST_WINDOW_RECORDS",
                    Integer.toString(activeMaxFilesPerRun)
            ));
            manifestWindow = BusRawParquetManifest.readWindow(
                    legacyManifestFile,
                    Math.max(0L, state.legacyManifestOffset),
                    manifestWindowRecords
            );
            candidateFiles = manifestWindow.records;
            System.out.printf(
                    "BusRawParquetCompactionJob: legacy manifest window offset=%d nextOffset=%d endOffset=%d records=%d reachedEnd=%s%n",
                    Math.max(0L, state.legacyManifestOffset),
                    manifestWindow.nextOffset,
                    manifestWindow.endOffset,
                    candidateFiles.size(),
                    manifestWindow.reachedEnd
            );
        } else if (useManifestListing && Files.exists(manifestFile)) {
            int manifestWindowRecords = Integer.parseInt(System.getenv().getOrDefault(
                    "BUS_COMPACTED_PARQUET_MANIFEST_WINDOW_RECORDS",
                    Integer.toString(activeMaxFilesPerRun)
            ));
            manifestWindow = BusRawParquetManifest.readWindow(
                    manifestFile,
                    Math.max(0L, state.manifestOffset),
                    manifestWindowRecords
            );
            candidateFiles = manifestWindow.records;
            System.out.printf(
                    "BusRawParquetCompactionJob: manifest window offset=%d nextOffset=%d records=%d%n",
                    Math.max(0L, state.manifestOffset),
                    manifestWindow.nextOffset,
                    candidateFiles.size()
            );
        } else {
            candidateFiles = useFindListing
                        ? listRecentParquetFilesWithFind(inputDir, modifiedSince)
                        : recursiveListing
                        ? IncrementalParquetSupport.listRecentParquetFiles(inputDir, modifiedSince)
                        : IncrementalParquetSupport.listRecentParquetFilesFlat(inputDir, modifiedSince);
        }
        List<IncrementalParquetSupport.ParquetFileInfo> selectedNewFiles = manifestWindow == null
                ? IncrementalParquetSupport.selectNewFiles(candidateFiles, state.lastProcessedModifiedAt, state.lastProcessedPath)
                : candidateFiles;
        System.out.printf(
                "BusRawParquetCompactionJob: listed %d candidates, %d selected new files%n",
                candidateFiles.size(),
                selectedNewFiles.size()
        );

        int cappedSize = Math.min(selectedNewFiles.size(), Math.max(1, activeMaxFilesPerRun));
        int startIndex = newestFirst ? Math.max(0, selectedNewFiles.size() - cappedSize) : 0;
        int endIndex = newestFirst ? selectedNewFiles.size() : cappedSize;
        List<IncrementalParquetSupport.ParquetFileInfo> windowFiles =
                new ArrayList<>(selectedNewFiles.subList(startIndex, endIndex));
        List<IncrementalParquetSupport.ParquetFileInfo> newFiles =
                filterReadableRawParquetFiles(windowFiles, minRawParquetBytes);

        if (!newFiles.isEmpty() && !isBatchReady(newFiles, activeMinBatchFiles, activeMinBatchBytes, activeMaxOpenLag, activeAllowSmallTail)) {
            return;
        }

        if (newFiles.isEmpty()) {
            if (windowFiles.isEmpty()) {
                System.out.println("BusRawParquetCompactionJob: no new parquet files to compact");
                if (manifestWindow != null && !candidateFiles.isEmpty()) {
                    updateState(stateFile, state, List.of(), manifestWindow, useLegacyManifest);
                } else if (manifestWindow != null && useLegacyManifest && manifestWindow.reachedEnd) {
                    state.legacyManifestComplete = true;
                    updateState(stateFile, state, List.of(), manifestWindow, true);
                }
            } else {
                System.out.println("BusRawParquetCompactionJob: no readable new parquet files to compact");
                updateState(stateFile, state, windowFiles, manifestWindow, useLegacyManifest);
            }
            return;
        }

        String batchId = "compact-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID();
        Path tempOutputDir = outputDir.getParent().resolve(outputDir.getFileName() + "-batch-tmp").resolve(batchId);
        Files.createDirectories(tempOutputDir.getParent());
        System.out.printf(
                "BusRawParquetCompactionJob: compacting %d files out of %d available since %s into %s%n",
                newFiles.size(),
                selectedNewFiles.size(),
                modifiedSince,
                tempOutputDir
        );

        Class.forName("org.duckdb.DuckDBDriver");
        int stagedReadChunkFiles = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_STAGED_READ_CHUNK_FILES",
                "0"
        ));
        long stagedReadChunkSleepMillis = Long.parseLong(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_STAGED_READ_CHUNK_SLEEP_MILLIS",
                "0"
        ));
        Path duckDbFile = null;
        String duckDbUrl = "jdbc:duckdb:";
        if (stagedReadChunkFiles > 0 && newFiles.size() > stagedReadChunkFiles) {
            Path duckDbTempDir = Path.of(System.getenv().getOrDefault(
                    "BUS_COMPACTED_PARQUET_DUCKDB_TEMP_DIR",
                    outputDir.getParent().resolve(outputDir.getFileName() + "-duckdb-tmp").toString()
            ));
            Files.createDirectories(duckDbTempDir);
            duckDbFile = duckDbTempDir.resolve(batchId + ".duckdb");
            duckDbUrl = "jdbc:duckdb:" + duckDbFile.toAbsolutePath();
        }
        try (Connection connection = DriverManager.getConnection(duckDbUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=" + Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_THREADS", "4")));
            if (stagedReadChunkFiles > 0 && newFiles.size() > stagedReadChunkFiles) {
                compactWithStagedReads(statement, newFiles, tempOutputDir, stagedReadChunkFiles, stagedReadChunkSleepMillis);
            } else {
                statement.execute(buildCopySql(newFiles, tempOutputDir));
            }
        }
        if (duckDbFile != null) {
            Files.deleteIfExists(duckDbFile);
            Files.deleteIfExists(Path.of(duckDbFile.toString() + ".wal"));
        }
        movePartitionedBatch(tempOutputDir, outputDir, batchId);

        updateState(stateFile, state, windowFiles, manifestWindow, useLegacyManifest);
    }

    private static boolean isBatchReady(
            List<IncrementalParquetSupport.ParquetFileInfo> files,
            int minBatchFiles,
            long minBatchBytes,
            Duration maxOpenLag,
            boolean allowSmallTail
    ) {
        if (allowSmallTail) {
            return true;
        }
        long totalBytes = 0L;
        Instant oldestModifiedAt = null;
        Instant newestModifiedAt = null;
        for (IncrementalParquetSupport.ParquetFileInfo file : files) {
            Instant modifiedAt = IncrementalParquetSupport.parseInstant(file.modifiedAt);
            if (modifiedAt != null && (oldestModifiedAt == null || modifiedAt.isBefore(oldestModifiedAt))) {
                oldestModifiedAt = modifiedAt;
            }
            if (modifiedAt != null && (newestModifiedAt == null || modifiedAt.isAfter(newestModifiedAt))) {
                newestModifiedAt = modifiedAt;
            }
            try {
                totalBytes += Files.size(Path.of(file.path));
            } catch (Exception ignored) {
                // The readability filter already checked the file; size races are handled by the next run.
            }
        }
        boolean readyByCount = files.size() >= minBatchFiles;
        boolean readyByBytes = totalBytes >= minBatchBytes;
        boolean readyByLag = oldestModifiedAt != null && !oldestModifiedAt.isAfter(Instant.now().minus(maxOpenLag));
        if (readyByCount || readyByBytes || readyByLag) {
            System.out.printf(
                    "BusRawParquetCompactionJob: batch ready files=%d bytes=%d oldest=%s newest=%s "
                            + "readyByCount=%s readyByBytes=%s readyByLag=%s%n",
                    files.size(),
                    totalBytes,
                    oldestModifiedAt,
                    newestModifiedAt,
                    readyByCount,
                    readyByBytes,
                    readyByLag
            );
            return true;
        }
        System.out.printf(
                "BusRawParquetCompactionJob: waiting for larger fresh batch files=%d bytes=%d oldest=%s newest=%s "
                        + "minFiles=%d minBytes=%d maxOpenLag=%s%n",
                files.size(),
                totalBytes,
                oldestModifiedAt,
                newestModifiedAt,
                minBatchFiles,
                minBatchBytes,
                maxOpenLag
        );
        return false;
    }

    private static List<IncrementalParquetSupport.ParquetFileInfo> filterReadableRawParquetFiles(
            List<IncrementalParquetSupport.ParquetFileInfo> files,
            long minRawParquetBytes
    ) {
        List<IncrementalParquetSupport.ParquetFileInfo> result = new ArrayList<>();
        int skipped = 0;
        for (IncrementalParquetSupport.ParquetFileInfo file : files) {
            Path path = Path.of(file.path);
            try {
                long size = Files.size(path);
                if (size < minRawParquetBytes) {
                    skipped++;
                    System.out.printf(
                            "BusRawParquetCompactionJob: skipping unreadable raw parquet %s (%d bytes < %d)%n",
                            file.path,
                            size,
                            minRawParquetBytes
                    );
                    continue;
                }
                result.add(file);
            } catch (Exception exception) {
                skipped++;
                System.out.printf(
                        "BusRawParquetCompactionJob: skipping unreadable raw parquet %s (%s)%n",
                        file.path,
                        exception.getMessage()
                );
            }
        }
        if (skipped > 0) {
            System.out.printf(
                    "BusRawParquetCompactionJob: skipped %d unreadable raw parquet files, %d remain%n",
                    skipped,
                    result.size()
            );
        }
        return result;
    }

    private static void compactWithStagedReads(
            Statement statement,
            List<IncrementalParquetSupport.ParquetFileInfo> files,
            Path outputDir,
            int chunkFiles,
            long sleepMillis
    ) throws Exception {
        System.out.printf(
                Locale.US,
                "BusRawParquetCompactionJob: staged read compacting files=%d chunkFiles=%d sleepMillis=%d%n",
                files.size(),
                chunkFiles,
                sleepMillis
        );
        for (int start = 0; start < files.size(); start += chunkFiles) {
            int end = Math.min(files.size(), start + chunkFiles);
            List<IncrementalParquetSupport.ParquetFileInfo> chunk = files.subList(start, end);
            String sql = (start == 0 ? "CREATE TABLE compacted_raw AS " : "INSERT INTO compacted_raw ")
                    + buildCompactedSelectSql(chunk);
            statement.execute(sql);
            System.out.printf(
                    Locale.US,
                    "BusRawParquetCompactionJob: staged read loaded chunk start=%d end=%d total=%d%n",
                    start,
                    end,
                    files.size()
            );
            if (sleepMillis > 0 && end < files.size()) {
                Thread.sleep(sleepMillis);
            }
        }
        statement.execute("COPY compacted_raw TO '" + sqlEscape(outputDir.toAbsolutePath().toString()) + "' "
                + "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (serviceDate))");
        statement.execute("DROP TABLE compacted_raw");
    }

    private static String buildCopySql(List<IncrementalParquetSupport.ParquetFileInfo> files, Path outputDir) {
        return "COPY (" + buildCompactedSelectSql(files) + ") TO '"
                + sqlEscape(outputDir.toAbsolutePath().toString()) + "' "
                + "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (serviceDate))";
    }

    private static String buildCompactedSelectSql(List<IncrementalParquetSupport.ParquetFileInfo> files) {
        String fileList = files.stream()
                .map(file -> "'" + sqlEscape(file.path) + "'")
                .collect(Collectors.joining(", ", "[", "]"));

        return "SELECT "
                + "plate, internalRouteId, realRouteNumber, latitude, longitude, "
                + "eventTime, max(speed) AS speed, "
                + "min(timestamp) AS timestamp, min(readableTime) AS readableTime, "
                + "min(sourceTimestamp) AS sourceTimestamp, min(sourceReadableTime) AS sourceReadableTime, "
                + "min(filename) AS sourceFile, "
                + "CAST(eventTime + INTERVAL '3 hours' AS DATE) AS serviceDate "
                + "FROM read_parquet(" + fileList + ", union_by_name=true, filename=true) "
                + "WHERE eventTime IS NOT NULL"
                + " AND timestamp IS NOT NULL"
                + " AND plate IS NOT NULL"
                + " AND internalRouteId IS NOT NULL"
                + " AND latitude IS NOT NULL"
                + " AND longitude IS NOT NULL"
                + " GROUP BY internalRouteId, realRouteNumber, plate, eventTime, latitude, longitude";
    }

    private static void movePartitionedBatch(Path tempOutputDir, Path outputDir, String batchId) throws Exception {
        AtomicInteger movedFiles = new AtomicInteger();
        try (var paths = Files.walk(tempOutputDir)) {
            for (Path sourceFile : paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .collect(Collectors.toList())) {
                Path relativeParent = tempOutputDir.relativize(sourceFile.getParent());
                Path targetDir = outputDir.resolve(relativeParent);
                Files.createDirectories(targetDir);
                String targetName = batchId + "-" + movedFiles.incrementAndGet() + ".parquet";
                Files.move(sourceFile, targetDir.resolve(targetName), StandardCopyOption.ATOMIC_MOVE);
            }
        }
        deleteRecursively(tempOutputDir);
        System.out.printf("BusRawParquetCompactionJob: moved %d partitioned parquet files into %s%n", movedFiles.get(), outputDir);
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path current : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(current);
            }
        }
    }

    private static List<IncrementalParquetSupport.ParquetFileInfo> listRecentParquetFilesWithFind(
            Path inputDir,
            Instant modifiedSince
    ) throws Exception {
        Instant searchSince = modifiedSince.minusMillis(1);
        double epochSeconds = searchSince.toEpochMilli() / 1000.0;
        Process process = new ProcessBuilder(
                "find",
                inputDir.toAbsolutePath().toString(),
                "-ignore_readdir_race",
                "-maxdepth",
                "1",
                "-type",
                "f",
                "-name",
                "*.parquet",
                "-newermt",
                String.format(Locale.US, "@%.3f", epochSeconds),
                "-printf",
                "%T@ %p\\0"
        ).start();

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        process.getInputStream().transferTo(stdout);
        process.getErrorStream().transferTo(stderr);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("find failed with exit code " + exitCode + ": "
                    + stderr.toString(StandardCharsets.UTF_8));
        }

        List<IncrementalParquetSupport.ParquetFileInfo> result = new ArrayList<>();
        for (String record : stdout.toString(StandardCharsets.UTF_8).split("\\u0000")) {
            if (record.isBlank()) {
                continue;
            }
            int separator = record.indexOf(' ');
            if (separator <= 0 || separator >= record.length() - 1) {
                continue;
            }
            double modifiedAtSeconds = Double.parseDouble(record.substring(0, separator));
            long epochSecond = (long) modifiedAtSeconds;
            long nanos = Math.round((modifiedAtSeconds - epochSecond) * 1_000_000_000L);

            IncrementalParquetSupport.ParquetFileInfo file = new IncrementalParquetSupport.ParquetFileInfo();
            file.modifiedAt = Instant.ofEpochSecond(epochSecond, nanos).toString();
            file.path = Path.of(record.substring(separator + 1)).toAbsolutePath().toString();
            result.add(file);
        }
        result.sort(Comparator
                .comparing((IncrementalParquetSupport.ParquetFileInfo file) -> IncrementalParquetSupport.parseInstant(file.modifiedAt))
                .thenComparing(file -> file.path));
        return result;
    }

    private static String sqlEscape(String value) {
        return value.replace("'", "''");
    }

    private static CompactionState loadState(Path stateFile) throws Exception {
        if (!Files.exists(stateFile)) {
            return new CompactionState();
        }
        return MAPPER.readValue(stateFile.toFile(), CompactionState.class);
    }

    private static void updateState(
            Path stateFile,
            CompactionState state,
            List<IncrementalParquetSupport.ParquetFileInfo> processedFiles,
            BusRawParquetManifest.ManifestWindow manifestWindow,
            boolean legacyManifest
    ) throws Exception {
        Long manifestOffset = manifestWindow == null ? null : manifestWindow.nextOffset;
        if (processedFiles.isEmpty() && manifestOffset == null) {
            return;
        }
        state.updatedAt = Instant.now().toString();
        if (!processedFiles.isEmpty()) {
            IncrementalParquetSupport.ParquetFileInfo last = processedFiles.get(processedFiles.size() - 1);
            state.lastProcessedModifiedAt = last.modifiedAt;
            state.lastProcessedPath = last.path;
        }
        if (manifestOffset != null) {
            if (legacyManifest) {
                state.legacyManifestOffset = manifestOffset;
                if (manifestWindow.reachedEnd) {
                    state.legacyManifestComplete = true;
                }
            } else {
                state.manifestOffset = manifestOffset;
            }
        }

        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static final class CompactionState {
        public String updatedAt;
        public String lastProcessedModifiedAt;
        public String lastProcessedPath;
        public long manifestOffset;
        public long legacyManifestOffset;
        public boolean legacyManifestComplete;
    }
}
