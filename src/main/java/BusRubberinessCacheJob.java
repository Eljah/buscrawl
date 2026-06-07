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

public class BusRubberinessCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));

    public static void main(String[] args) throws Exception {
        Path dataRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path cacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_RUBBERINESS_CACHE_FILE",
                "./var/bus/dashboard-cache/rubberiness.json"
        ));
        Path mapConfigFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_MAP_CONFIG_FILE",
                cacheFile.resolveSibling("map-config.json").toString()
        ));
        Path tileRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TILE_ROOT",
                cacheFile.resolveSibling("tiles").toString()
        ));
        Files.createDirectories(cacheFile.getParent());

        Path dailyStopDir = dataRoot.resolve("daily-rubber-stop");
        if (!Files.exists(dailyStopDir)) {
            writeJsonAtomic(cacheFile, emptyPayload("Rubberiness parquet datasets not found under " + dataRoot));
            return;
        }

        RouteTopology topology = RouteTopology.load(null);
        LocalDate yesterday = LocalDate.now(CITY_ZONE).minusDays(1);
        LocalDate today = LocalDate.now(CITY_ZONE);

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
            payload.put("terminalPolicies", List.of(
                    Map.of("id", "with-terminals", "label", "With Terminals"),
                    Map.of("id", "without-terminals", "label", "Without Terminals")
            ));

            payload.put("stopData", buildSection(
                    connection,
                    dataRoot.resolve("daily-rubber-stop"),
                    dataRoot.resolve("summary-rubber-stop-all-days"),
                    dataRoot.resolve("summary-rubber-stop-by-weekday"),
                    yesterday,
                    RubberMode.STOP
            ));
            payload.put("stopRouteData", buildSection(
                    connection,
                    dataRoot.resolve("daily-rubber-stop-route"),
                    dataRoot.resolve("summary-rubber-stop-route-all-days"),
                    dataRoot.resolve("summary-rubber-stop-route-by-weekday"),
                    yesterday,
                    RubberMode.STOP_ROUTE
            ));
            payload.put("routeData", buildSection(
                    connection,
                    dataRoot.resolve("daily-rubber-route"),
                    dataRoot.resolve("summary-rubber-route-all-days"),
                    dataRoot.resolve("summary-rubber-route-by-weekday"),
                    yesterday,
                    RubberMode.ROUTE
            ));
            payload.put("vehicleData", buildSection(
                    connection,
                    dataRoot.resolve("daily-rubber-vehicle"),
                    dataRoot.resolve("summary-rubber-vehicle-all-days"),
                    dataRoot.resolve("summary-rubber-vehicle-by-weekday"),
                    yesterday,
                    RubberMode.VEHICLE
            ));
            payload.put("longStopData", buildLongRubberSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, RubberMode.STOP));
            payload.put("longStopRouteData", buildLongRubberSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, RubberMode.STOP_ROUTE));
            payload.put("longRouteData", buildLongRubberSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, RubberMode.ROUTE));
            payload.put("longVehicleData", buildLongRubberSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, RubberMode.VEHICLE));
            payload.put("heatmapData", buildDwellPointSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, 0));
            payload.put("longHeatmapData", buildDwellPointSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, 60));
            payload.put("longDwellPointData", buildDwellPointSection(connection, dataRoot.resolve("dwell-events"), yesterday, today, 60));

            writeJsonAtomic(cacheFile, payload);
            DashboardOverlayTileRenderer.renderRubberinessHeatmapTiles(cacheFile, mapConfigFile, tileRoot);
        }
    }

    private static Map<String, Object> buildLongRubberSection(
            Connection connection,
            Path dwellEventsDir,
            LocalDate yesterday,
            LocalDate today,
            RubberMode mode
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadLongRubberRows(connection, dwellEventsDir, mode, yesterday, null, today));
        section.put("allDays", loadLongRubberRows(connection, dwellEventsDir, mode, null, null, today));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadLongRubberRows(connection, dwellEventsDir, mode, null, weekdayIso, today));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadLongRubberRows(
            Connection connection,
            Path dwellEventsDir,
            RubberMode mode,
            LocalDate serviceDate,
            Integer weekdayIso,
            LocalDate today
    ) throws Exception {
        if (!Files.exists(dwellEventsDir)) {
            return List.of();
        }
        boolean summary = serviceDate == null;
        String dimensions = mode.dwellEventDimensions();
        StringBuilder sql = new StringBuilder();
        sql.append("WITH policy_events AS (");
        sql.append("SELECT 'with-terminals' AS terminalPolicy, * FROM read_parquet(?) ");
        sql.append("UNION ALL SELECT 'without-terminals' AS terminalPolicy, * FROM read_parquet(?) WHERE NOT isTerminalStop");
        sql.append(") SELECT ");
        sql.append(dimensions);
        sql.append(", SUM(dwellTimeSeconds) AS totalDwellSeconds, MAX(dwellTimeSeconds) AS maxDwellSeconds, ");
        sql.append("ROUND(AVG(dwellTimeSeconds), 0) AS averageDwellSeconds, COUNT(*) AS visitCount");
        if (summary) {
            sql.append(", COUNT(DISTINCT serviceDate) AS sampleDays");
        }
        sql.append(" FROM policy_events WHERE dwellTimeSeconds > 60 ");
        if (serviceDate != null) {
            sql.append("AND CAST(serviceDate AS VARCHAR) = ? ");
        } else {
            sql.append("AND CAST(serviceDate AS VARCHAR) < ? ");
            if (weekdayIso != null) {
                sql.append("AND weekdayIso = ? ");
            }
        }
        sql.append("GROUP BY ");
        sql.append(dimensions);
        sql.append(" ORDER BY totalDwellSeconds DESC, maxDwellSeconds DESC");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            String glob = parquetGlob(dwellEventsDir);
            statement.setString(1, glob);
            statement.setString(2, glob);
            int parameterIndex = 3;
            if (serviceDate != null) {
                statement.setString(parameterIndex++, serviceDate.toString());
            } else {
                statement.setString(parameterIndex++, today.toString());
                if (weekdayIso != null) {
                    statement.setInt(parameterIndex++, weekdayIso);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mode.readRow(rs, summary));
                }
                return rows;
            }
        }
    }

    private static Map<String, Object> buildDwellPointSection(
            Connection connection,
            Path dwellEventsDir,
            LocalDate yesterday,
            LocalDate today,
            int minDwellSeconds
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadDwellPointRows(connection, dwellEventsDir, yesterday, null, today, minDwellSeconds));
        section.put("allDays", loadDwellPointRows(connection, dwellEventsDir, null, null, today, minDwellSeconds));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadDwellPointRows(connection, dwellEventsDir, null, weekdayIso, today, minDwellSeconds));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadDwellPointRows(
            Connection connection,
            Path dwellEventsDir,
            LocalDate serviceDate,
            Integer weekdayIso,
            LocalDate today,
            int minDwellSeconds
    ) throws Exception {
        if (!Files.exists(dwellEventsDir)) {
            return List.of();
        }
        boolean summary = serviceDate == null;
        StringBuilder sql = new StringBuilder();
        sql.append("WITH policy_events AS (");
        sql.append("SELECT 'with-terminals' AS terminalPolicy, * FROM read_parquet(?) ");
        sql.append("UNION ALL SELECT 'without-terminals' AS terminalPolicy, * FROM read_parquet(?) WHERE NOT isTerminalStop");
        sql.append(") SELECT terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, ");
        sql.append("SUM(dwellTimeSeconds) AS totalDwellSeconds, MAX(dwellTimeSeconds) AS maxDwellSeconds, ");
        sql.append("ROUND(AVG(dwellTimeSeconds), 0) AS averageDwellSeconds, COUNT(*) AS visitCount, ");
        if (summary) {
            sql.append("COUNT(DISTINCT serviceDate) AS sampleDays, ");
        }
        sql.append("MAX(enteredStopAt) AS latestEnteredStopAt, ");
        sql.append("max_by(plate, enteredStopAt) AS latestPlate, max_by(routeNumber, enteredStopAt) AS latestRouteNumber ");
        sql.append("FROM policy_events WHERE dwellTimeSeconds > ? ");
        if (serviceDate != null) {
            sql.append("AND CAST(serviceDate AS VARCHAR) = ? ");
        } else {
            sql.append("AND CAST(serviceDate AS VARCHAR) < ? ");
            if (weekdayIso != null) {
                sql.append("AND weekdayIso = ? ");
            }
        }
        sql.append("GROUP BY terminalPolicy, stopId, stopName, stopLatitude, stopLongitude ");
        sql.append("ORDER BY totalDwellSeconds DESC, maxDwellSeconds DESC LIMIT 2000");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            String glob = parquetGlob(dwellEventsDir);
            statement.setString(1, glob);
            statement.setString(2, glob);
            statement.setInt(3, minDwellSeconds);
            int parameterIndex = 4;
            if (serviceDate != null) {
                statement.setString(parameterIndex++, serviceDate.toString());
            } else {
                statement.setString(parameterIndex++, today.toString());
                if (weekdayIso != null) {
                    statement.setInt(parameterIndex++, weekdayIso);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("terminalPolicy", rs.getString(1));
                    row.put("stopId", rs.getString(2));
                    row.put("stopName", rs.getString(3));
                    row.put("latitude", rs.getDouble(4));
                    row.put("longitude", rs.getDouble(5));
                    row.put("totalDwellSeconds", rs.getLong(6));
                    row.put("maxDwellSeconds", rs.getLong(7));
                    row.put("averageDwellSeconds", rs.getInt(8));
                    row.put("visitCount", rs.getLong(9));
                    int latestIndex = 10;
                    if (summary) {
                        row.put("sampleDays", rs.getLong(10));
                        latestIndex = 11;
                    }
                    row.put("latestEnteredStopAt", toIsoString(rs.getObject(latestIndex)));
                    row.put("latestPlate", rs.getString(latestIndex + 1));
                    row.put("latestRouteNumber", rs.getString(latestIndex + 2));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static Map<String, Object> buildSection(
            Connection connection,
            Path dailyDir,
            Path summaryAllDir,
            Path summaryWeekdayDir,
            LocalDate yesterday,
            RubberMode mode
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadRows(connection, parquetGlob(dailyDir), mode, yesterday, null));
        section.put("allDays", loadRows(connection, parquetGlob(summaryAllDir), mode, null, null));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadRows(connection, parquetGlob(summaryWeekdayDir), mode, null, weekdayIso));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadRows(
            Connection connection,
            String parquetGlob,
            RubberMode mode,
            LocalDate serviceDate,
            Integer weekdayIso
    ) throws Exception {
        boolean hasSampleDays = serviceDate == null;
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(mode.selectColumns(hasSampleDays));
        sql.append(" FROM read_parquet(?) ");
        if (serviceDate != null) {
            sql.append("WHERE CAST(serviceDate AS VARCHAR) = ? ");
        } else if (weekdayIso != null) {
            sql.append("WHERE weekdayIso = ? ");
        }
        sql.append(" ORDER BY totalDwellSeconds DESC, maxDwellSeconds DESC");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setString(1, parquetGlob);
            if (serviceDate != null) {
                statement.setString(2, serviceDate.toString());
            } else if (weekdayIso != null) {
                statement.setInt(2, weekdayIso);
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(mode.readRow(rs, hasSampleDays));
                }
                return rows;
            }
        }
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
        payload.put("terminalPolicies", List.of());
        Map<String, Object> emptySection = Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        );
        payload.put("stopData", emptySection);
        payload.put("stopRouteData", emptySection);
        payload.put("routeData", emptySection);
        payload.put("vehicleData", emptySection);
        payload.put("longStopData", emptySection);
        payload.put("longStopRouteData", emptySection);
        payload.put("longRouteData", emptySection);
        payload.put("longVehicleData", emptySection);
        payload.put("heatmapData", emptySection);
        payload.put("longHeatmapData", emptySection);
        payload.put("longDwellPointData", emptySection);
        return payload;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
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

    private enum RubberMode {
        STOP(
                "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ),
        STOP_ROUTE(
                "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ),
        ROUTE(
                "terminalPolicy, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "terminalPolicy, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ),
        VEHICLE(
                "terminalPolicy, plate, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "terminalPolicy, plate, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        );

        private final String dailySelectColumns;
        private final String summarySelectColumns;

        RubberMode(String dailySelectColumns, String summarySelectColumns) {
            this.dailySelectColumns = dailySelectColumns;
            this.summarySelectColumns = summarySelectColumns;
        }

        String selectColumns(boolean hasSampleDays) {
            return hasSampleDays ? summarySelectColumns : dailySelectColumns;
        }

        String dwellEventDimensions() {
            switch (this) {
                case STOP:
                    return "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude";
                case STOP_ROUTE:
                    return "terminalPolicy, stopId, stopName, stopLatitude, stopLongitude, routeNumber";
                case ROUTE:
                    return "terminalPolicy, routeNumber";
                case VEHICLE:
                    return "terminalPolicy, plate, routeNumber";
                default:
                    throw new IllegalStateException("Unexpected value: " + this);
            }
        }

        Map<String, Object> readRow(ResultSet rs, boolean hasSampleDays) throws Exception {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("terminalPolicy", rs.getString(1));
            int metricStartIndex;
            switch (this) {
                case STOP:
                    row.put("stopId", rs.getString(2));
                    row.put("stopName", rs.getString(3));
                    row.put("latitude", rs.getDouble(4));
                    row.put("longitude", rs.getDouble(5));
                    metricStartIndex = 6;
                    break;
                case STOP_ROUTE:
                    row.put("stopId", rs.getString(2));
                    row.put("stopName", rs.getString(3));
                    row.put("latitude", rs.getDouble(4));
                    row.put("longitude", rs.getDouble(5));
                    row.put("routeNumber", rs.getString(6));
                    metricStartIndex = 7;
                    break;
                case ROUTE:
                    row.put("routeNumber", rs.getString(2));
                    metricStartIndex = 3;
                    break;
                case VEHICLE:
                    row.put("plate", rs.getString(2));
                    row.put("routeNumber", rs.getString(3));
                    metricStartIndex = 4;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + this);
            }
            row.put("totalDwellSeconds", rs.getLong(metricStartIndex));
            row.put("maxDwellSeconds", rs.getLong(metricStartIndex + 1));
            row.put("averageDwellSeconds", rs.getInt(metricStartIndex + 2));
            row.put("visitCount", rs.getLong(metricStartIndex + 3));
            if (hasSampleDays) {
                row.put("sampleDays", rs.getLong(metricStartIndex + 4));
            }
            return row;
        }
    }
}
