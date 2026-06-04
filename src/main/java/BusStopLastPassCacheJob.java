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

public class BusStopLastPassCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));

    public static void main(String[] args) throws Exception {
        Path dataRoot = Path.of(System.getenv().getOrDefault("BUS_STOP_LAST_PASS_DIR", "./var/bus/stop-last-pass"));
        Path cacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_STOP_LAST_PASS_CACHE_FILE",
                "./var/bus/dashboard-cache/stop-last-pass.json"
        ));

        Files.createDirectories(cacheFile.getParent());

        Path dailyRouteDir = dataRoot.resolve("daily-route");
        Path dailyStopDir = dataRoot.resolve("daily-stop");
        Path summaryRouteAllDir = dataRoot.resolve("summary-route-all-days");
        Path summaryRouteWeekdayDir = dataRoot.resolve("summary-route-by-weekday");
        Path summaryStopAllDir = dataRoot.resolve("summary-stop-all-days");
        Path summaryStopWeekdayDir = dataRoot.resolve("summary-stop-by-weekday");

        if (!Files.exists(dailyRouteDir) || !Files.exists(dailyStopDir)) {
            writeJsonAtomic(cacheFile, emptyPayload("Stop last-pass parquet datasets not found under " + dataRoot));
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
            payload.put("routeShapes", topology.buildRouteShapes());
            payload.put("stops", topology.buildStops());

            Map<String, Object> routes = new LinkedHashMap<>();
            routes.put("previousDay", loadRoutePreviousDay(connection, parquetGlob(dailyRouteDir), yesterday));
            routes.put("allDaysAverage", loadRouteAverages(connection, parquetGlob(summaryRouteAllDir), null));
            routes.put("weekdayAverage", loadRouteAveragesByWeekday(connection, parquetGlob(summaryRouteWeekdayDir)));
            payload.put("routes", routes);

            Map<String, Object> stops = new LinkedHashMap<>();
            stops.put("previousDay", loadStopPreviousDay(connection, parquetGlob(dailyStopDir), yesterday));
            stops.put("allDaysAverage", loadStopAverages(connection, parquetGlob(summaryStopAllDir), null));
            stops.put("weekdayAverage", loadStopAveragesByWeekday(connection, parquetGlob(summaryStopWeekdayDir)));
            payload.put("stopsData", stops);

            writeJsonAtomic(cacheFile, payload);
        }
    }

    private static List<Map<String, Object>> loadRoutePreviousDay(Connection connection, String parquetGlob, LocalDate serviceDate) throws Exception {
        String sql = "SELECT routeNumber, lastPassAt " +
                "FROM read_parquet(?) " +
                "WHERE CAST(serviceDate AS VARCHAR) = ? " +
                "ORDER BY lastPassAt DESC, routeNumber ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            statement.setString(2, serviceDate.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("routeNumber", resultSet.getString(1));
                    row.put("lastPassAt", toIsoString(resultSet.getObject(2)));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static List<Map<String, Object>> loadStopPreviousDay(Connection connection, String parquetGlob, LocalDate serviceDate) throws Exception {
        String sql = "SELECT stopId, stopName, stopLatitude, stopLongitude, lastPassAt, lastRouteNumber " +
                "FROM read_parquet(?) " +
                "WHERE CAST(serviceDate AS VARCHAR) = ? " +
                "ORDER BY lastPassAt DESC, stopName ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            statement.setString(2, serviceDate.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("stopId", resultSet.getString(1));
                    row.put("stopName", resultSet.getString(2));
                    row.put("latitude", resultSet.getDouble(3));
                    row.put("longitude", resultSet.getDouble(4));
                    row.put("lastPassAt", toIsoString(resultSet.getObject(5)));
                    row.put("lastRouteNumber", resultSet.getString(6));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static List<Map<String, Object>> loadRouteAverages(Connection connection, String parquetGlob, Integer weekdayIso) throws Exception {
        String sql = "SELECT routeNumber, averageLastPassSeconds, sampleDays " +
                "FROM read_parquet(?) " +
                (weekdayIso == null ? "" : "WHERE weekdayIso = ? ") +
                "ORDER BY averageLastPassSeconds DESC, routeNumber ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            if (weekdayIso != null) {
                statement.setInt(2, weekdayIso);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    int averageSeconds = resultSet.getInt(2);
                    row.put("routeNumber", resultSet.getString(1));
                    row.put("averageLastPassSeconds", averageSeconds);
                    row.put("averageLastPassTime", formatSecondsOfDay(averageSeconds));
                    row.put("sampleDays", resultSet.getLong(3));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static Map<String, List<Map<String, Object>>> loadRouteAveragesByWeekday(Connection connection, String parquetGlob) throws Exception {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (int weekday = 1; weekday <= 7; weekday++) {
            result.put(String.valueOf(weekday), loadRouteAverages(connection, parquetGlob, weekday));
        }
        return result;
    }

    private static List<Map<String, Object>> loadStopAverages(Connection connection, String parquetGlob, Integer weekdayIso) throws Exception {
        String sql = "SELECT stopId, stopName, stopLatitude, stopLongitude, averageLastPassSeconds, sampleDays " +
                "FROM read_parquet(?) " +
                (weekdayIso == null ? "" : "WHERE weekdayIso = ? ") +
                "ORDER BY averageLastPassSeconds DESC, stopName ASC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob);
            if (weekdayIso != null) {
                statement.setInt(2, weekdayIso);
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (resultSet.next()) {
                    int averageSeconds = resultSet.getInt(5);
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("stopId", resultSet.getString(1));
                    row.put("stopName", resultSet.getString(2));
                    row.put("latitude", resultSet.getDouble(3));
                    row.put("longitude", resultSet.getDouble(4));
                    row.put("averageLastPassSeconds", averageSeconds);
                    row.put("averageLastPassTime", formatSecondsOfDay(averageSeconds));
                    row.put("sampleDays", resultSet.getLong(6));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static Map<String, List<Map<String, Object>>> loadStopAveragesByWeekday(Connection connection, String parquetGlob) throws Exception {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (int weekday = 1; weekday <= 7; weekday++) {
            result.put(String.valueOf(weekday), loadStopAverages(connection, parquetGlob, weekday));
        }
        return result;
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

    private static Map<String, Object> emptyPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        payload.put("timezone", CITY_ZONE.getId());
        payload.put("yesterday", LocalDate.now(CITY_ZONE).minusDays(1).toString());
        payload.put("weekdays", buildWeekdays());
        payload.put("routeShapes", List.of());
        payload.put("stops", List.of());
        payload.put("routes", Map.of(
                "previousDay", List.of(),
                "allDaysAverage", List.of(),
                "weekdayAverage", Map.of()
        ));
        payload.put("stopsData", Map.of(
                "previousDay", List.of(),
                "allDaysAverage", List.of(),
                "weekdayAverage", Map.of()
        ));
        return payload;
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

    private static String formatSecondsOfDay(int seconds) {
        int safeSeconds = Math.max(0, Math.min(86_399, seconds));
        int hours = safeSeconds / 3600;
        int minutes = (safeSeconds % 3600) / 60;
        int remainingSeconds = safeSeconds % 60;
        return String.format("%02d:%02d:%02d", hours, minutes, remainingSeconds);
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
