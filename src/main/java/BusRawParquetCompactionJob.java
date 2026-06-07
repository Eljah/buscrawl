import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

        List<IncrementalParquetSupport.ParquetFileInfo> candidateFiles =
                IncrementalParquetSupport.listRecentParquetFiles(inputDir, modifiedSince);
        List<IncrementalParquetSupport.ParquetFileInfo> selectedNewFiles =
                IncrementalParquetSupport.selectNewFiles(candidateFiles, state.lastProcessedModifiedAt, state.lastProcessedPath);

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
                + "plate, internalRouteId, realRouteNumber, latitude, longitude, eventTime, speed, "
                + "timestamp, readableTime, sourceTimestamp, sourceReadableTime, "
                + "filename AS sourceFile "
                + "FROM read_parquet(" + fileList + ", union_by_name=true, filename=true) "
                + "WHERE eventTime IS NOT NULL"
                + ") TO '" + sqlEscape(outputFile.toAbsolutePath().toString()) + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
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
