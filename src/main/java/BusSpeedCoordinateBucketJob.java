import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class BusSpeedCoordinateBucketJob {
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));

    public static void main(String[] args) throws Exception {
        Path rawParquetDir = Path.of(System.getenv().getOrDefault(
                "BUS_SPEED_MAP_RAW_PARQUET_DIR",
                "/home/eljah/data/buscrawl/bus-data-parquet-compacted"
        ));
        Path outputRoot = Path.of(System.getenv().getOrDefault(
                "BUS_SPEED_MAP_DIR",
                "./var/bus/speed-map"
        )).resolve("coordinate-heatmap");
        int minZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_MIN_ZOOM", "11"));
        int maxZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_MAX_ZOOM", "15"));
        int bucketPixels = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_HEATMAP_BUCKET_PIXELS", "6"));
        int threads = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_DUCKDB_THREADS", "4"));

        if (!Files.exists(rawParquetDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + rawParquetDir);
        }

        Files.createDirectories(outputRoot);
        clearDirectory(outputRoot);
        Files.createDirectories(outputRoot);

        LocalDate yesterday = LocalDate.now(CITY_ZONE).minusDays(1);
        String parquetGlob = rawParquetDir.toAbsolutePath().toString().replace('\\', '/') + "/**/*.parquet";

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA threads=" + threads);
            statement.execute("PRAGMA preserve_insertion_order=false");

            for (int zoom = minZoom; zoom <= maxZoom; zoom++) {
                buildDailyBuckets(statement, parquetGlob, zoom, bucketPixels);
                writeDailyVariant(statement, outputRoot.resolve("previous").resolve("zoom=" + zoom),
                        "serviceDate = DATE '" + yesterday + "'");
                writeSummaryVariant(statement, outputRoot.resolve("all").resolve("zoom=" + zoom), "TRUE");
                for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
                    writeSummaryVariant(statement, outputRoot.resolve("weekday").resolve(String.valueOf(weekdayIso)).resolve("zoom=" + zoom),
                            "weekdayIso = " + weekdayIso);
                }
            }
        }
    }

    private static void buildDailyBuckets(
            Statement statement,
            String parquetGlob,
            int zoom,
            int bucketPixels
    ) throws Exception {
        double scale = 256.0 * Math.pow(2.0, zoom);
        String worldX = "((longitude + 180.0) / 360.0) * " + scale;
        String worldY = "(0.5 - ln((1 + sin(radians(latitude))) / (1 - sin(radians(latitude)))) / (4 * pi())) * " + scale;
        String sql = "CREATE OR REPLACE TEMP TABLE speed_coordinate_daily_buckets AS "
                + "WITH raw_points AS ("
                + "SELECT latitude, longitude, speed, CAST(eventTime + INTERVAL '3 hours' AS DATE) AS serviceDate, "
                + "CAST(strftime(eventTime + INTERVAL '3 hours', '%u') AS INTEGER) AS weekdayIso "
                + "FROM read_parquet('" + sqlEscape(parquetGlob) + "', union_by_name=true) "
                + "WHERE eventTime IS NOT NULL "
                + "AND latitude BETWEEN 54.0 AND 57.0 "
                + "AND longitude BETWEEN 47.0 AND 52.0 "
                + "AND speed BETWEEN 0 AND 140"
                + "), buckets AS ("
                + "SELECT "
                + "serviceDate, weekdayIso, "
                + zoom + " AS zoom, "
                + bucketPixels + " AS bucketSizePx, "
                + "CAST(floor((" + worldX + ") / " + bucketPixels + ") AS INTEGER) AS bucketX, "
                + "CAST(floor((" + worldY + ") / " + bucketPixels + ") AS INTEGER) AS bucketY, "
                + "speed "
                + "FROM raw_points"
                + ") "
                + "SELECT serviceDate, weekdayIso, zoom, bucketSizePx, bucketX, bucketY, "
                + "COUNT(*) AS sampleCount, "
                + "ROUND(AVG(speed), 2) AS avgSpeedKmh, "
                + "MIN(speed) AS minSpeedKmh, "
                + "MAX(speed) AS maxSpeedKmh "
                + "FROM buckets "
                + "GROUP BY serviceDate, weekdayIso, zoom, bucketSizePx, bucketX, bucketY";
        statement.execute(sql);
    }

    private static void writeDailyVariant(Statement statement, Path outputDir, String filter) throws Exception {
        Files.createDirectories(outputDir);
        Path outputFile = outputDir.resolve("part.parquet");
        String sql = "COPY ("
                + "SELECT zoom, bucketSizePx, bucketX, bucketY, sampleCount, avgSpeedKmh, minSpeedKmh, maxSpeedKmh "
                + "FROM speed_coordinate_daily_buckets "
                + "WHERE " + filter
                + ") TO '" + sqlEscape(outputFile.toAbsolutePath().toString()) + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        statement.execute(sql);
    }

    private static void writeSummaryVariant(Statement statement, Path outputDir, String filter) throws Exception {
        Files.createDirectories(outputDir);
        Path outputFile = outputDir.resolve("part.parquet");
        String sql = "COPY ("
                + "SELECT zoom, bucketSizePx, bucketX, bucketY, "
                + "SUM(sampleCount) AS sampleCount, "
                + "ROUND(SUM(avgSpeedKmh * sampleCount) / NULLIF(SUM(sampleCount), 0), 2) AS avgSpeedKmh, "
                + "MIN(minSpeedKmh) AS minSpeedKmh, "
                + "MAX(maxSpeedKmh) AS maxSpeedKmh "
                + "FROM speed_coordinate_daily_buckets "
                + "WHERE " + filter + " "
                + "GROUP BY zoom, bucketSizePx, bucketX, bucketY"
                + ") TO '" + sqlEscape(outputFile.toAbsolutePath().toString()) + "' (FORMAT PARQUET, COMPRESSION ZSTD)";
        statement.execute(sql);
    }

    private static void clearDirectory(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            stream.forEach(paths::add);
        }
        paths.sort((left, right) -> right.getNameCount() - left.getNameCount());
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    private static String sqlEscape(String value) {
        return value.replace("'", "''");
    }
}
