import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
        int initialLookbackDays = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_INITIAL_LOOKBACK_DAYS", "3"));
        String bootstrapStrategy = System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_BOOTSTRAP_MODE", "full-history");
        boolean newestFirst = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_NEWEST_FIRST", "false"));

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
        System.out.printf(
                "BusRawParquetCompactionJob: listing raw parquet files in %s since %s (recursive=%s, find=%s)%n",
                inputDir,
                modifiedSince,
                recursiveListing,
                useFindListing
        );
        List<IncrementalParquetSupport.ParquetFileInfo> candidateFiles =
                useFindListing
                        ? listRecentParquetFilesWithFind(inputDir, modifiedSince)
                        : recursiveListing
                        ? IncrementalParquetSupport.listRecentParquetFiles(inputDir, modifiedSince)
                        : IncrementalParquetSupport.listRecentParquetFilesFlat(inputDir, modifiedSince);
        List<IncrementalParquetSupport.ParquetFileInfo> selectedNewFiles =
                IncrementalParquetSupport.selectNewFiles(candidateFiles, state.lastProcessedModifiedAt, state.lastProcessedPath);
        System.out.printf(
                "BusRawParquetCompactionJob: listed %d candidates, %d selected new files%n",
                candidateFiles.size(),
                selectedNewFiles.size()
        );

        int cappedSize = Math.min(selectedNewFiles.size(), Math.max(1, maxFilesPerRun));
        int startIndex = newestFirst ? Math.max(0, selectedNewFiles.size() - cappedSize) : 0;
        int endIndex = newestFirst ? selectedNewFiles.size() : cappedSize;
        List<IncrementalParquetSupport.ParquetFileInfo> newFiles =
                new ArrayList<>(selectedNewFiles.subList(startIndex, endIndex));

        if (newFiles.isEmpty()) {
            System.out.println("BusRawParquetCompactionJob: no new parquet files to compact");
            return;
        }

        Path outputFile = outputDir.resolve("compact-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID() + ".parquet");
        System.out.printf(
                "BusRawParquetCompactionJob: compacting %d files out of %d available since %s into %s%n",
                newFiles.size(),
                selectedNewFiles.size(),
                modifiedSince,
                outputFile
        );

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=" + Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_THREADS", "4")));
            statement.execute(buildCopySql(newFiles, outputFile));
        }

        updateState(stateFile, state, newFiles);
    }

    private static String buildCopySql(List<IncrementalParquetSupport.ParquetFileInfo> files, Path outputFile) {
        String fileList = files.stream()
                .map(file -> "'" + sqlEscape(file.path) + "'")
                .collect(Collectors.joining(", ", "[", "]"));

        return "COPY ("
                + "SELECT "
                + "plate, internalRouteId, realRouteNumber, latitude, longitude, "
                + "min(eventTime) AS eventTime, speed, "
                + "min(timestamp) AS timestamp, min(readableTime) AS readableTime, "
                + "sourceTimestamp, min(sourceReadableTime) AS sourceReadableTime, "
                + "min(filename) AS sourceFile "
                + "FROM read_parquet(" + fileList + ", union_by_name=true, filename=true) "
                + "WHERE eventTime IS NOT NULL"
                + " AND timestamp IS NOT NULL"
                + " AND plate IS NOT NULL"
                + " AND internalRouteId IS NOT NULL"
                + " AND latitude IS NOT NULL"
                + " AND longitude IS NOT NULL"
                + " GROUP BY internalRouteId, realRouteNumber, plate, latitude, longitude, speed, sourceTimestamp"
                + ") TO '" + sqlEscape(outputFile.toAbsolutePath().toString()) + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
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
            List<IncrementalParquetSupport.ParquetFileInfo> processedFiles
    ) throws Exception {
        if (processedFiles.isEmpty()) {
            return;
        }
        IncrementalParquetSupport.ParquetFileInfo last = processedFiles.get(processedFiles.size() - 1);
        state.updatedAt = Instant.now().toString();
        state.lastProcessedModifiedAt = last.modifiedAt;
        state.lastProcessedPath = last.path;

        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public static final class CompactionState {
        public String updatedAt;
        public String lastProcessedModifiedAt;
        public String lastProcessedPath;
    }
}
