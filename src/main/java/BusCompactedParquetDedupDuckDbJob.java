import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.UUID;

public class BusCompactedParquetDedupDuckDbJob {
    public static void main(String[] args) throws Exception {
        Path inputDir = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_DEDUP_INPUT_DIR",
                "./var/bus/bus-data-parquet-compacted"
        ));
        Path outputDir = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_DEDUP_OUTPUT_DIR",
                "./var/bus/bus-data-parquet-compacted-dedup"
        ));
        Path tempRoot = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_DEDUP_TEMP_ROOT",
                outputDir.getParent().resolve("bus-data-parquet-compacted-dedup-tmp").toString()
        ));
        int threads = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_DEDUP_THREADS", "2"));

        if (!Files.exists(inputDir)) {
            throw new IllegalStateException("Compacted parquet input directory not found: " + inputDir);
        }
        Files.createDirectories(outputDir);
        Files.createDirectories(tempRoot);

        Path tempOutput = tempRoot.resolve("dedup-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID());
        List<String> inputFiles;
        try (var paths = Files.walk(inputDir)) {
            inputFiles = paths
                    .filter(Files::isRegularFile)
                    .map(Path::toString)
                    .filter(path -> path.endsWith(".parquet"))
                    .map(path -> path.replace('\\', '/'))
                    .sorted()
                    .collect(Collectors.toList());
        }
        if (inputFiles.isEmpty()) {
            throw new IllegalStateException("No compacted parquet files found in " + inputDir);
        }
        String inputFileList = inputFiles.stream()
                .map(path -> "'" + sqlEscape(path) + "'")
                .collect(Collectors.joining(", ", "[", "]"));
        String outputPath = tempOutput.toAbsolutePath().toString().replace('\\', '/');

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=" + threads);
            statement.execute("PRAGMA memory_limit='" + System.getenv().getOrDefault("BUS_COMPACTED_DEDUP_MEMORY_LIMIT", "8GB") + "'");

            long inputRows = scalarLong(statement, "SELECT count(*) FROM read_parquet(" + inputFileList + ", union_by_name=true, filename=true)");
            long uniqueRows = scalarLong(statement,
                    "SELECT count(*) FROM ("
                            + "SELECT 1 FROM read_parquet(" + inputFileList + ", union_by_name=true, filename=true) "
                            + "WHERE timestamp IS NOT NULL "
                            + "AND plate IS NOT NULL "
                            + "AND internalRouteId IS NOT NULL "
                            + "AND eventTime IS NOT NULL "
                            + "AND latitude IS NOT NULL "
                            + "AND longitude IS NOT NULL "
                            + "GROUP BY internalRouteId, realRouteNumber, plate, eventTime, latitude, longitude"
                            + ") t"
            );
            System.out.printf(
                    "BusCompactedParquetDedupDuckDbJob: inputRows=%d uniqueRows=%d dupFactor=%.2f%n",
                    inputRows,
                    uniqueRows,
                    inputRows / (double) Math.max(1L, uniqueRows)
            );

            String copySql = "COPY ("
                    + "SELECT "
                    + "plate, internalRouteId, realRouteNumber, latitude, longitude, "
                    + "eventTime, max(speed) AS speed, min(timestamp) AS timestamp, "
                    + "min(readableTime) AS readableTime, min(sourceTimestamp) AS sourceTimestamp, "
                    + "min(sourceReadableTime) AS sourceReadableTime, "
                    + "min(filename) AS sourceFile, "
                    + "CAST(eventTime + INTERVAL '3 hours' AS DATE) AS serviceDate "
                    + "FROM read_parquet(" + inputFileList + ", union_by_name=true, filename=true) "
                    + "WHERE timestamp IS NOT NULL "
                    + "AND plate IS NOT NULL "
                    + "AND internalRouteId IS NOT NULL "
                    + "AND eventTime IS NOT NULL "
                    + "AND latitude IS NOT NULL "
                    + "AND longitude IS NOT NULL "
                    + "GROUP BY internalRouteId, realRouteNumber, plate, eventTime, latitude, longitude"
                    + ") TO '" + sqlEscape(outputPath) + "' "
                    + "(FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (serviceDate))";
            statement.execute(copySql);
        }

        System.out.printf(
                "BusCompactedParquetDedupDuckDbJob: wrote deduplicated compacted parquet to %s%n",
                tempOutput.toAbsolutePath()
        );
        System.out.printf(
                "BusCompactedParquetDedupDuckDbJob: validate then move %s to %s during cutover%n",
                tempOutput.toAbsolutePath(),
                outputDir.toAbsolutePath()
        );
    }

    private static long scalarLong(Statement statement, String sql) throws Exception {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                return 0L;
            }
            return resultSet.getLong(1);
        }
    }

    private static String sqlEscape(String value) {
        return value.replace("'", "''");
    }
}
