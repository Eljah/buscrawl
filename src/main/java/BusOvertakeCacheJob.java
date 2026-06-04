import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BusOvertakeCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));

    public static void main(String[] args) throws Exception {
        Path dataRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path cacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_OVERTAKE_CACHE_FILE",
                "./var/bus/dashboard-cache/overtake.json"
        ));

        Files.createDirectories(cacheFile.getParent());

        Path dailyDir = dataRoot.resolve("daily-overtake-segment");
        Path summaryAllDir = dataRoot.resolve("summary-overtake-all-days");
        Path summaryWeekdayDir = dataRoot.resolve("summary-overtake-by-weekday");
        if (!Files.exists(dailyDir)) {
            writeJsonAtomic(cacheFile, emptyPayload("Overtake parquet datasets not found under " + dataRoot));
            return;
        }

        RouteTopology topology = RouteTopology.load(null);
        LocalDate yesterday = LocalDate.now(CITY_ZONE).minusDays(1);

        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
            payload.put("status", "ok");
            payload.put("timezone", CITY_ZONE.getId());
            payload.put("yesterday", yesterday.toString());
            payload.put("weekdays", buildWeekdays());
            payload.put("segmentShapes", topology.buildSegmentShapes());

            Map<String, Object> segments = new LinkedHashMap<>();
            segments.put("previousDay", loadPreviousDay(connection, parquetGlob(dailyDir), yesterday));
            segments.put("allDays", loadSummary(connection, parquetGlob(summaryAllDir), null));
            segments.put("weekday", loadSummaryByWeekday(connection, parquetGlob(summaryWeekdayDir)));
            payload.put("segments", segments);

            writeJsonAtomic(cacheFile, payload);
        }
    }

    private static List<Map<String, Object>> loadPreviousDay(Connection connection, String parquetGlob, LocalDate serviceDate) throws Exception {
        String sql = "SELECT segmentId, routeNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, " +
                "endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, " +
                "averageOvertakerDurationSeconds, averageOvertakenDurationSeconds " +
                "FROM read_parquet(?) WHERE CAST(serviceDate AS VARCHAR) = ? " +
                "ORDER BY overtakeCount DESC, latestOvertakeAt DESC, routeNumber ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            statement.setString(2, serviceDate.toString());
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(readOvertakeRow(rs, false));
                }
                return rows;
            }
        }
    }

    private static List<Map<String, Object>> loadSummary(Connection connection, String parquetGlob, Integer weekdayIso) throws Exception {
        String sql = "SELECT segmentId, routeNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, " +
                "endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, " +
                "averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt " +
                "FROM read_parquet(?) " +
                (weekdayIso == null ? "" : "WHERE weekdayIso = ? ") +
                "ORDER BY totalOvertakes DESC, averageDailyOvertakes DESC, routeNumber ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            if (weekdayIso != null) {
                statement.setInt(2, weekdayIso);
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(readOvertakeRow(rs, true));
                }
                return rows;
            }
        }
    }

    private static Map<String, List<Map<String, Object>>> loadSummaryByWeekday(Connection connection, String parquetGlob) throws Exception {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (int weekday = 1; weekday <= 7; weekday++) {
            result.put(String.valueOf(weekday), loadSummary(connection, parquetGlob, weekday));
        }
        return result;
    }

    private static Map<String, Object> readOvertakeRow(ResultSet rs, boolean summary) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("segmentId", rs.getString(1));
        row.put("routeNumber", rs.getString(2));
        row.put("startStopId", rs.getString(3));
        row.put("startStopName", rs.getString(4));
        row.put("startStopLatitude", rs.getDouble(5));
        row.put("startStopLongitude", rs.getDouble(6));
        row.put("endStopId", rs.getString(7));
        row.put("endStopName", rs.getString(8));
        row.put("endStopLatitude", rs.getDouble(9));
        row.put("endStopLongitude", rs.getDouble(10));
        row.put("distanceMeters", rs.getDouble(11));
        if (summary) {
            row.put("totalOvertakes", rs.getLong(12));
            row.put("sampleDays", rs.getLong(13));
            row.put("averageDailyOvertakes", rs.getDouble(14));
            row.put("maxDailyOvertakes", rs.getLong(15));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(16)));
        } else {
            row.put("overtakeCount", rs.getLong(12));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(13)));
            row.put("averageOvertakerDurationSeconds", rs.getInt(14));
            row.put("averageOvertakenDurationSeconds", rs.getInt(15));
        }
        return row;
    }

    private static List<Map<String, Object>> buildWeekdays() {
        List<Map<String, Object>> weekdays = new ArrayList<>();
        for (int weekday = 1; weekday <= 7; weekday++) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", weekday);
            item.put("label", LocalDate.of(2024, 1, weekday).getDayOfWeek().getDisplayName(TextStyle.SHORT_STANDALONE, new Locale("ru", "RU")));
            weekdays.add(item);
        }
        return weekdays;
    }

    private static String parquetGlob(Path dir) {
        return dir.toAbsolutePath().toString().replace('\\', '/') + "/**/*.parquet";
    }

    private static String toIsoString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toInstant().toString();
        }
        return String.valueOf(value);
    }

    private static Map<String, Object> emptyPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("timezone", CITY_ZONE.getId());
        payload.put("yesterday", LocalDate.now(CITY_ZONE).minusDays(1).toString());
        payload.put("weekdays", buildWeekdays());
        payload.put("segmentShapes", List.of());
        payload.put("segments", Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        ));
        return payload;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
