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
        Files.createDirectories(cacheFile.getParent());

        Path dailyStopDir = dataRoot.resolve("daily-rubber-stop");
        Path dailyRouteDir = dataRoot.resolve("daily-rubber-route");
        Path dailyVehicleDir = dataRoot.resolve("daily-rubber-vehicle");
        if (!Files.exists(dailyStopDir)) {
            writeJsonAtomic(cacheFile, emptyPayload("Rubberiness parquet datasets not found under " + dataRoot));
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

            payload.put("stopsData", buildSection(
                    connection,
                    dailyStopDir,
                    dataRoot.resolve("summary-rubber-stop-all-days"),
                    dataRoot.resolve("summary-rubber-stop-by-weekday"),
                    yesterday,
                    RubberMode.STOP
            ));
            payload.put("routesData", buildSection(
                    connection,
                    dailyRouteDir,
                    dataRoot.resolve("summary-rubber-route-all-days"),
                    dataRoot.resolve("summary-rubber-route-by-weekday"),
                    yesterday,
                    RubberMode.ROUTE
            ));
            payload.put("vehiclesData", buildSection(
                    connection,
                    dailyVehicleDir,
                    dataRoot.resolve("summary-rubber-vehicle-all-days"),
                    dataRoot.resolve("summary-rubber-vehicle-by-weekday"),
                    yesterday,
                    RubberMode.VEHICLE
            ));

            writeJsonAtomic(cacheFile, payload);
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
        sql.append("ORDER BY totalDwellSeconds DESC, maxDwellSeconds DESC");

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
        payload.put("stopsData", emptySection());
        payload.put("routesData", emptySection());
        payload.put("vehiclesData", emptySection());
        return payload;
    }

    private static Map<String, Object> emptySection() {
        return Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        );
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private enum RubberMode {
        STOP(
                "stopId, stopName, stopLatitude, stopLongitude, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "stopId, stopName, stopLatitude, stopLongitude, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ) {
            @Override
            Map<String, Object> readRow(ResultSet rs, boolean hasSampleDays) throws Exception {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("stopId", rs.getString(1));
                row.put("stopName", rs.getString(2));
                row.put("latitude", rs.getDouble(3));
                row.put("longitude", rs.getDouble(4));
                fillMetrics(row, rs, 5, hasSampleDays);
                return row;
            }
        },
        ROUTE(
                "routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ) {
            @Override
            Map<String, Object> readRow(ResultSet rs, boolean hasSampleDays) throws Exception {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("routeNumber", rs.getString(1));
                fillMetrics(row, rs, 2, hasSampleDays);
                return row;
            }
        },
        VEHICLE(
                "plate, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount",
                "plate, routeNumber, totalDwellSeconds, maxDwellSeconds, averageDwellSeconds, visitCount, sampleDays"
        ) {
            @Override
            Map<String, Object> readRow(ResultSet rs, boolean hasSampleDays) throws Exception {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("plate", rs.getString(1));
                row.put("routeNumber", rs.getString(2));
                fillMetrics(row, rs, 3, hasSampleDays);
                return row;
            }
        };

        private final String dailySelectColumns;
        private final String summarySelectColumns;

        RubberMode(String dailySelectColumns, String summarySelectColumns) {
            this.dailySelectColumns = dailySelectColumns;
            this.summarySelectColumns = summarySelectColumns;
        }

        abstract Map<String, Object> readRow(ResultSet rs, boolean hasSampleDays) throws Exception;

        String selectColumns(boolean hasSampleDays) {
            return hasSampleDays ? summarySelectColumns : dailySelectColumns;
        }

        void fillMetrics(Map<String, Object> row, ResultSet rs, int metricStartIndex, boolean hasSampleDays) throws Exception {
            row.put("totalDwellSeconds", rs.getLong(metricStartIndex));
            row.put("maxDwellSeconds", rs.getLong(metricStartIndex + 1));
            row.put("averageDwellSeconds", rs.getInt(metricStartIndex + 2));
            row.put("visitCount", rs.getLong(metricStartIndex + 3));
            if (hasSampleDays) {
                row.put("sampleDays", rs.getLong(metricStartIndex + 4));
            }
        }
    }
}
