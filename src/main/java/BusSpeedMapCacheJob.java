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

public class BusSpeedMapCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));
    private static final int ROW_LIMIT = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_CACHE_ROW_LIMIT", "20000"));
    private static final int HEATMAP_ROW_LIMIT = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_HEATMAP_ROW_LIMIT", "0"));

    public static void main(String[] args) throws Exception {
        Path speedMapDir = Path.of(System.getenv().getOrDefault("BUS_SPEED_MAP_DIR", "./var/bus/speed-map"));
        Path cacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_SPEED_MAP_CACHE_FILE",
                "./var/bus/dashboard-cache/speed-map.json"
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
        if (!Files.exists(speedMapDir.resolve("daily-speed-point"))) {
            writeJsonAtomic(cacheFile, emptyPayload("Speed map parquet datasets not found under " + speedMapDir));
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
            payload.put("segmentShapes", topology.buildSegmentShapes());
            payload.put("routeShapes", topology.buildRouteShapes());
            payload.put("pointData", buildSection(connection, speedMapDir, "speed-point", yesterday, today, Mode.POINT));
            payload.put("physicalSegmentData", buildSection(connection, speedMapDir, "speed-physical-segment", yesterday, today, Mode.PHYSICAL_SEGMENT));
            payload.put("routeSegmentData", buildSection(connection, speedMapDir, "speed-route-segment", yesterday, today, Mode.ROUTE_SEGMENT));
            payload.put("routeData", buildSection(connection, speedMapDir, "speed-route", yesterday, today, Mode.ROUTE));
            writeJsonAtomic(cacheFile, payload);
            Map<String, Object> coordinateHeatmapData = buildCoordinateHeatmapSection(connection, speedMapDir, yesterday, today);
            DashboardOverlayTileRenderer.renderSpeedCoordinateHeatmapTiles(coordinateHeatmapData, mapConfigFile, tileRoot);
        }
    }

    private static Map<String, Object> buildSection(Connection connection, Path root, String name, LocalDate yesterday, LocalDate today, Mode mode) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadRows(connection, root.resolve("daily-" + name), yesterday, null, today, mode, false));
        section.put("allDays", loadRows(connection, root.resolve("summary-" + name + "-all-days"), null, null, today, mode, true));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadRows(connection, root.resolve("summary-" + name + "-by-weekday"), null, weekdayIso, today, mode, true));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static Map<String, Object> buildCoordinateHeatmapSection(Connection connection, Path root, LocalDate yesterday, LocalDate today) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        Path coordinateRoot = root.resolve("coordinate-heatmap");
        section.put("previousDay", loadCoordinateRows(connection, coordinateRoot.resolve("previous")));
        section.put("allDays", loadCoordinateRows(connection, coordinateRoot.resolve("all")));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadCoordinateRows(connection, coordinateRoot.resolve("weekday").resolve(String.valueOf(weekdayIso))));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadCoordinateRows(
            Connection connection,
            Path dir
    ) throws Exception {
        if (!Files.exists(dir)) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT zoom, bucketSizePx, bucketX, bucketY, ");
        sql.append("sampleCount, ");
        sql.append("avgSpeedKmh, minSpeedKmh, maxSpeedKmh ");
        sql.append("FROM read_parquet(?) ");
        sql.append("ORDER BY zoom ASC, ");
        sql.append("sampleCount DESC ");
        if (HEATMAP_ROW_LIMIT > 0) {
            sql.append("LIMIT ? ");
        }

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setString(1, parquetGlob(dir));
            int index = 2;
            if (HEATMAP_ROW_LIMIT > 0) {
                statement.setInt(index, HEATMAP_ROW_LIMIT);
            }
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    int i = 1;
                    row.put("zoom", rs.getInt(i++));
                    row.put("bucketSizePx", rs.getInt(i++));
                    row.put("bucketX", rs.getInt(i++));
                    row.put("bucketY", rs.getInt(i++));
                    row.put("sampleCount", rs.getLong(i++));
                    row.put("avgSpeedKmh", rs.getDouble(i++));
                    row.put("minSpeedKmh", rs.getDouble(i++));
                    row.put("maxSpeedKmh", rs.getDouble(i));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static List<Map<String, Object>> loadRows(
            Connection connection,
            Path dir,
            LocalDate serviceDate,
            Integer weekdayIso,
            LocalDate today,
            Mode mode,
            boolean summary
    ) throws Exception {
        if (!Files.exists(dir)) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        appendColumns(sql, mode, summary);
        sql.append(" FROM read_parquet(?) WHERE 1=1 ");
        if (serviceDate != null) {
            sql.append("AND CAST(serviceDate AS VARCHAR) = ? ");
        } else if (weekdayIso != null) {
            sql.append("AND weekdayIso = ? ");
        } else if (!summary) {
            sql.append("AND CAST(serviceDate AS VARCHAR) < ? ");
        }
        sql.append("ORDER BY avgSpeedKmh ASC, ");
        sql.append(summary ? "totalSamples" : "sampleCount").append(" DESC LIMIT ?");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setString(1, parquetGlob(dir));
            int index = 2;
            if (serviceDate != null) {
                statement.setString(index++, serviceDate.toString());
            } else if (weekdayIso != null) {
                statement.setInt(index++, weekdayIso);
            } else if (!summary) {
                statement.setString(index++, today.toString());
            }
            statement.setInt(index, ROW_LIMIT);
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(readRow(rs, mode, summary));
                }
                return rows;
            }
        }
    }

    private static void appendColumns(StringBuilder sql, Mode mode, boolean summary) {
        switch (mode) {
            case POINT:
                sql.append("pointLatitude, pointLongitude, ");
                break;
            case PHYSICAL_SEGMENT:
                sql.append("physicalSegmentId, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, ");
                break;
            case ROUTE_SEGMENT:
                sql.append("segmentId, internalRouteId, routeNumber, direction, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, ");
                break;
            case ROUTE:
                sql.append("routeNumber, ");
                break;
        }
        sql.append(summary ? "totalSamples, sampleDays, " : "sampleCount, ");
        sql.append("avgSpeedKmh, minSpeedKmh, maxSpeedKmh, latestTripAt");
    }

    private static Map<String, Object> readRow(ResultSet rs, Mode mode, boolean summary) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        int i = 1;
        switch (mode) {
            case POINT:
                row.put("pointLatitude", rs.getDouble(i++));
                row.put("pointLongitude", rs.getDouble(i++));
                break;
            case PHYSICAL_SEGMENT:
                row.put("physicalSegmentId", rs.getString(i++));
                putSegment(row, rs, i);
                i += 8;
                break;
            case ROUTE_SEGMENT:
                row.put("segmentId", rs.getString(i++));
                row.put("internalRouteId", rs.getString(i++));
                row.put("routeNumber", rs.getString(i++));
                row.put("direction", rs.getInt(i++));
                putSegment(row, rs, i);
                i += 8;
                break;
            case ROUTE:
                row.put("routeNumber", rs.getString(i++));
                break;
        }
        if (summary) {
            row.put("totalSamples", rs.getLong(i++));
            row.put("sampleDays", rs.getLong(i++));
        } else {
            row.put("sampleCount", rs.getLong(i++));
        }
        row.put("avgSpeedKmh", rs.getDouble(i++));
        row.put("minSpeedKmh", rs.getDouble(i++));
        row.put("maxSpeedKmh", rs.getDouble(i++));
        row.put("latestTripAt", toIsoString(rs.getObject(i)));
        return row;
    }

    private static void putSegment(Map<String, Object> row, ResultSet rs, int index) throws Exception {
        row.put("startStopId", rs.getString(index));
        row.put("startStopName", rs.getString(index + 1));
        row.put("startStopLatitude", rs.getDouble(index + 2));
        row.put("startStopLongitude", rs.getDouble(index + 3));
        row.put("endStopId", rs.getString(index + 4));
        row.put("endStopName", rs.getString(index + 5));
        row.put("endStopLatitude", rs.getDouble(index + 6));
        row.put("endStopLongitude", rs.getDouble(index + 7));
    }

    private static List<Map<String, Object>> buildWeekdays() {
        List<Map<String, Object>> weekdays = new ArrayList<>();
        for (int i = 1; i <= 7; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("weekdayIso", i);
            row.put("label", java.time.DayOfWeek.of(i).getDisplayName(TextStyle.FULL, Locale.forLanguageTag("ru-RU")));
            weekdays.add(row);
        }
        return weekdays;
    }

    private static String parquetGlob(Path dir) {
        return dir.toAbsolutePath() + "/**/*.parquet";
    }

    private static String toIsoString(Object value) {
        return value == null ? null : value.toString().replace(' ', 'T') + "Z";
    }

    private static Map<String, Object> emptyPayload(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "empty");
        payload.put("message", message);
        return payload;
    }

    private static void writeJsonAtomic(Path path, Map<String, Object> payload) throws Exception {
        Files.createDirectories(path.getParent());
        Path tempFile = Files.createTempFile(path.getParent(), path.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private enum Mode {
        POINT,
        PHYSICAL_SEGMENT,
        ROUTE_SEGMENT,
        ROUTE
    }
}
