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
    private static final int PHYSICAL_SEGMENT_VEHICLE_ROW_LIMIT = Integer.parseInt(System.getenv().getOrDefault(
            "BUS_OVERTAKE_CACHE_PHYSICAL_SEGMENT_VEHICLE_ROW_LIMIT",
            "1000"
    ));
    private static final int POINT_HEATMAP_ROW_LIMIT = Integer.parseInt(System.getenv().getOrDefault(
            "BUS_OVERTAKE_CACHE_POINT_HEATMAP_ROW_LIMIT",
            "2000"
    ));

    public static void main(String[] args) throws Exception {
        Path dataRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path cacheFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_OVERTAKE_CACHE_FILE",
                "./var/bus/dashboard-cache/overtake.json"
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

        Path dailySegmentDir = dataRoot.resolve("daily-overtake-segment");
        if (!Files.exists(dailySegmentDir)) {
            writeJsonAtomic(cacheFile, emptyPayload("Overtake parquet datasets not found under " + dataRoot));
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

            payload.put("segmentData", buildSection(
                    connection,
                    dataRoot.resolve("daily-overtake-segment"),
                    dataRoot.resolve("summary-overtake-all-days"),
                    dataRoot.resolve("summary-overtake-by-weekday"),
                    yesterday,
                    OvertakeMode.SEGMENT
            ));
            payload.put("segmentRouteData", buildSection(
                    connection,
                    dataRoot.resolve("daily-overtake-segment-route"),
                    dataRoot.resolve("summary-overtake-segment-route-all-days"),
                    dataRoot.resolve("summary-overtake-segment-route-by-weekday"),
                    yesterday,
                    OvertakeMode.SEGMENT_ROUTE
            ));
            payload.put("segmentVehicleData", buildSection(
                    connection,
                    dataRoot.resolve("daily-overtake-segment-vehicle"),
                    dataRoot.resolve("summary-overtake-segment-vehicle-all-days"),
                    dataRoot.resolve("summary-overtake-segment-vehicle-by-weekday"),
                    yesterday,
                    OvertakeMode.SEGMENT_VEHICLE
            ));
            payload.put("vehicleData", buildSection(
                    connection,
                    dataRoot.resolve("daily-overtake-vehicle"),
                    dataRoot.resolve("summary-overtake-vehicle-all-days"),
                    dataRoot.resolve("summary-overtake-vehicle-by-weekday"),
                    yesterday,
                    OvertakeMode.VEHICLE
            ));
            payload.put("vehicleDetails", Map.of());
            payload.put("pointHeatmapData", buildPointHeatmapSection(
                    connection,
                    dataRoot.resolve("overtake-events"),
                    yesterday,
                    today,
                    false
            ));
            payload.put("physicalSegmentData", buildPhysicalSection(
                    connection,
                    dataRoot.resolve("daily-physical-overtake-segment"),
                    dataRoot.resolve("summary-physical-overtake-segment-all-days"),
                    dataRoot.resolve("summary-physical-overtake-segment-by-weekday"),
                    yesterday,
                    true
            ));
            payload.put("physicalSegmentVehicleData", buildPhysicalSegmentVehicleSection(
                    connection,
                    dataRoot.resolve("daily-physical-overtake-segment-vehicle"),
                    dataRoot.resolve("summary-physical-overtake-segment-vehicle-all-days"),
                    dataRoot.resolve("summary-physical-overtake-segment-vehicle-by-weekday"),
                    yesterday
            ));
            payload.put("physicalVehicleData", buildPhysicalSection(
                    connection,
                    dataRoot.resolve("daily-physical-overtake-vehicle"),
                    dataRoot.resolve("summary-physical-overtake-vehicle-all-days"),
                    dataRoot.resolve("summary-physical-overtake-vehicle-by-weekday"),
                    yesterday,
                    false
            ));
            payload.put("physicalVehicleDetails", Map.of());
            payload.put("physicalPointHeatmapData", buildPointHeatmapSection(
                    connection,
                    dataRoot.resolve("physical-overtake-events"),
                    yesterday,
                    today,
                    true
            ));

            writeJsonAtomic(cacheFile, payload);
            DashboardOverlayTileRenderer.renderOvertakePointHeatmapTiles(cacheFile, mapConfigFile, tileRoot);
        }
    }

    private static Map<String, Object> buildPointHeatmapSection(
            Connection connection,
            Path eventsDir,
            LocalDate yesterday,
            LocalDate today,
            boolean physical
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadPointHeatmapRows(connection, eventsDir, yesterday, null, today, physical));
        section.put("allDays", loadPointHeatmapRows(connection, eventsDir, null, null, today, physical));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadPointHeatmapRows(connection, eventsDir, null, weekdayIso, today, physical));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadPointHeatmapRows(
            Connection connection,
            Path eventsDir,
            LocalDate serviceDate,
            Integer weekdayIso,
            LocalDate today,
            boolean physical
    ) throws Exception {
        if (!Files.exists(eventsDir)) {
            return List.of();
        }

        String overtakenRouteColumn = physical ? "overtakenRouteNumber" : "routeNumber";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append("ROUND((startStopLatitude + endStopLatitude) / 2, 3) AS pointLatitude, ");
        sql.append("ROUND((startStopLongitude + endStopLongitude) / 2, 3) AS pointLongitude, ");
        sql.append("COUNT(*) AS overtakeCount, ");
        sql.append("MAX(overtakeAt) AS latestOvertakeAt, ");
        sql.append("max_by(overtakerPlate, overtakeAt) AS latestOvertakerPlate, ");
        sql.append("max_by(overtakerRouteNumber, overtakeAt) AS latestOvertakerRouteNumber, ");
        sql.append("max_by(overtakenPlate, overtakeAt) AS latestOvertakenPlate, ");
        sql.append("max_by(").append(overtakenRouteColumn).append(", overtakeAt) AS latestOvertakenRouteNumber ");
        sql.append("FROM read_parquet(?) ");
        sql.append("WHERE startStopLatitude IS NOT NULL AND startStopLongitude IS NOT NULL ");
        sql.append("AND endStopLatitude IS NOT NULL AND endStopLongitude IS NOT NULL ");
        if (serviceDate != null) {
            sql.append("AND CAST(serviceDate AS VARCHAR) = ? ");
        } else {
            sql.append("AND CAST(serviceDate AS VARCHAR) < ? ");
            if (weekdayIso != null) {
                sql.append("AND weekdayIso = ? ");
            }
        }
        sql.append("GROUP BY pointLatitude, pointLongitude ");
        sql.append("ORDER BY overtakeCount DESC, latestOvertakeAt DESC ");
        sql.append("LIMIT ?");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setString(1, parquetGlob(eventsDir));
            int parameterIndex = 2;
            if (serviceDate != null) {
                statement.setString(parameterIndex++, serviceDate.toString());
            } else {
                statement.setString(parameterIndex++, today.toString());
                if (weekdayIso != null) {
                    statement.setInt(parameterIndex++, weekdayIso);
                }
            }
            statement.setInt(parameterIndex, POINT_HEATMAP_ROW_LIMIT);
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("pointLatitude", rs.getDouble(1));
                    row.put("pointLongitude", rs.getDouble(2));
                    row.put("overtakeCount", rs.getLong(3));
                    row.put("latestOvertakeAt", toIsoString(rs.getObject(4)));
                    row.put("latestOvertakerPlate", rs.getString(5));
                    row.put("latestOvertakerRouteNumber", rs.getString(6));
                    row.put("latestOvertakenPlate", rs.getString(7));
                    row.put("latestOvertakenRouteNumber", rs.getString(8));
                    rows.add(row);
                }
                return rows;
            }
        }
    }

    private static Map<String, Object> buildPhysicalSection(
            Connection connection,
            Path dailyDir,
            Path summaryAllDir,
            Path summaryWeekdayDir,
            LocalDate yesterday,
            boolean segmentMode
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadPhysicalRows(connection, parquetGlob(dailyDir), yesterday, null, segmentMode));
        section.put("allDays", loadPhysicalRows(connection, parquetGlob(summaryAllDir), null, null, segmentMode));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadPhysicalRows(connection, parquetGlob(summaryWeekdayDir), null, weekdayIso, segmentMode));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static Map<String, Object> buildPhysicalSegmentVehicleSection(
            Connection connection,
            Path dailyDir,
            Path summaryAllDir,
            Path summaryWeekdayDir,
            LocalDate yesterday
    ) throws Exception {
        Map<String, Object> section = new LinkedHashMap<>();
        section.put("previousDay", loadPhysicalSegmentVehicleRows(connection, parquetGlob(dailyDir), yesterday, null));
        section.put("allDays", loadPhysicalSegmentVehicleRows(connection, parquetGlob(summaryAllDir), null, null));
        Map<String, List<Map<String, Object>>> weekday = new LinkedHashMap<>();
        for (int weekdayIso = 1; weekdayIso <= 7; weekdayIso++) {
            weekday.put(String.valueOf(weekdayIso), loadPhysicalSegmentVehicleRows(connection, parquetGlob(summaryWeekdayDir), null, weekdayIso));
        }
        section.put("weekday", weekday);
        return section;
    }

    private static List<Map<String, Object>> loadPhysicalSegmentVehicleRows(
            Connection connection,
            String parquetGlob,
            LocalDate serviceDate,
            Integer weekdayIso
    ) throws Exception {
        boolean summary = serviceDate == null;
        String selectColumns = summary
                ? "physicalSegmentId, overtakerRouteNumber, overtakerPlate, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt, latestOvertakenPlate, latestOvertakenRouteNumber"
                : "physicalSegmentId, overtakerRouteNumber, overtakerPlate, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, latestOvertakenPlate, latestOvertakenRouteNumber, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds";
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(selectColumns);
        sql.append(" FROM read_parquet(?) ");
        if (serviceDate != null) {
            sql.append("WHERE CAST(serviceDate AS VARCHAR) = ? ");
        } else if (weekdayIso != null) {
            sql.append("WHERE weekdayIso = ? ");
        }
        sql.append("ORDER BY ");
        sql.append(summary ? "totalOvertakes DESC, averageDailyOvertakes DESC" : "overtakeCount DESC, latestOvertakeAt DESC");
        sql.append(" LIMIT ?");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setString(1, parquetGlob);
            int parameterIndex = 2;
            if (serviceDate != null) {
                statement.setString(parameterIndex++, serviceDate.toString());
            } else if (weekdayIso != null) {
                statement.setInt(parameterIndex++, weekdayIso);
            }
            statement.setInt(parameterIndex, PHYSICAL_SEGMENT_VEHICLE_ROW_LIMIT);
            try (ResultSet rs = statement.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    rows.add(readPhysicalSegmentVehicleRow(rs, summary));
                }
                return rows;
            }
        }
    }

    private static List<Map<String, Object>> loadPhysicalRows(
            Connection connection,
            String parquetGlob,
            LocalDate serviceDate,
            Integer weekdayIso,
            boolean segmentMode
    ) throws Exception {
        boolean summary = serviceDate == null;
        String selectColumns = segmentMode
                ? (summary
                ? "physicalSegmentId, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt, latestOvertakerPlate, latestOvertakerRouteNumber, latestOvertakenPlate, latestOvertakenRouteNumber"
                : "physicalSegmentId, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, latestOvertakerPlate, latestOvertakerRouteNumber, latestOvertakenPlate, latestOvertakenRouteNumber, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds")
                : (summary
                ? "overtakerRouteNumber, overtakerPlate, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt, latestStartStopName, latestEndStopName"
                : "overtakerRouteNumber, overtakerPlate, overtakeCount, latestOvertakeAt, latestStartStopName, latestEndStopName, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds");

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(selectColumns);
        sql.append(" FROM read_parquet(?) ");
        if (serviceDate != null) {
            sql.append("WHERE CAST(serviceDate AS VARCHAR) = ? ");
        } else if (weekdayIso != null) {
            sql.append("WHERE weekdayIso = ? ");
        }
        sql.append("ORDER BY ");
        sql.append(summary ? "totalOvertakes DESC, averageDailyOvertakes DESC" : "overtakeCount DESC, latestOvertakeAt DESC");

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
                    rows.add(segmentMode ? readPhysicalSegmentRow(rs, summary) : readPhysicalVehicleRow(rs, summary));
                }
                return rows;
            }
        }
    }

    private static Map<String, Object> readPhysicalSegmentRow(ResultSet rs, boolean summary) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("physicalSegmentId", rs.getString(1));
        row.put("segmentId", rs.getString(1));
        row.put("startStopId", rs.getString(2));
        row.put("startStopName", rs.getString(3));
        row.put("startStopLatitude", rs.getDouble(4));
        row.put("startStopLongitude", rs.getDouble(5));
        row.put("endStopId", rs.getString(6));
        row.put("endStopName", rs.getString(7));
        row.put("endStopLatitude", rs.getDouble(8));
        row.put("endStopLongitude", rs.getDouble(9));
        row.put("distanceMeters", rs.getDouble(10));
        if (summary) {
            row.put("totalOvertakes", rs.getLong(11));
            row.put("sampleDays", rs.getLong(12));
            row.put("averageDailyOvertakes", rs.getDouble(13));
            row.put("maxDailyOvertakes", rs.getLong(14));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(15)));
            row.put("latestOvertakerPlate", rs.getString(16));
            row.put("latestOvertakerRouteNumber", rs.getString(17));
            row.put("latestOvertakenPlate", rs.getString(18));
            row.put("latestOvertakenRouteNumber", rs.getString(19));
        } else {
            row.put("overtakeCount", rs.getLong(11));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(12)));
            row.put("latestOvertakerPlate", rs.getString(13));
            row.put("latestOvertakerRouteNumber", rs.getString(14));
            row.put("latestOvertakenPlate", rs.getString(15));
            row.put("latestOvertakenRouteNumber", rs.getString(16));
            row.put("averageOvertakerDurationSeconds", rs.getInt(17));
            row.put("averageOvertakenDurationSeconds", rs.getInt(18));
        }
        return row;
    }

    private static Map<String, Object> readPhysicalSegmentVehicleRow(ResultSet rs, boolean summary) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("physicalSegmentId", rs.getString(1));
        row.put("segmentId", rs.getString(1));
        row.put("overtakerRouteNumber", rs.getString(2));
        row.put("overtakerPlate", rs.getString(3));
        row.put("startStopId", rs.getString(4));
        row.put("startStopName", rs.getString(5));
        row.put("startStopLatitude", rs.getDouble(6));
        row.put("startStopLongitude", rs.getDouble(7));
        row.put("endStopId", rs.getString(8));
        row.put("endStopName", rs.getString(9));
        row.put("endStopLatitude", rs.getDouble(10));
        row.put("endStopLongitude", rs.getDouble(11));
        row.put("distanceMeters", rs.getDouble(12));
        if (summary) {
            row.put("totalOvertakes", rs.getLong(13));
            row.put("sampleDays", rs.getLong(14));
            row.put("averageDailyOvertakes", rs.getDouble(15));
            row.put("maxDailyOvertakes", rs.getLong(16));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(17)));
            row.put("latestOvertakenPlate", rs.getString(18));
            row.put("latestOvertakenRouteNumber", rs.getString(19));
        } else {
            row.put("overtakeCount", rs.getLong(13));
            row.put("latestOvertakeAt", toIsoString(rs.getObject(14)));
            row.put("latestOvertakenPlate", rs.getString(15));
            row.put("latestOvertakenRouteNumber", rs.getString(16));
            row.put("averageOvertakerDurationSeconds", rs.getInt(17));
            row.put("averageOvertakenDurationSeconds", rs.getInt(18));
        }
        return row;
    }

    private static Map<String, Object> readPhysicalVehicleRow(ResultSet rs, boolean summary) throws Exception {
        return OvertakeMode.VEHICLE.readRow(rs, summary);
    }

    private static Map<String, Object> buildSection(
            Connection connection,
            Path dailyDir,
            Path summaryAllDir,
            Path summaryWeekdayDir,
            LocalDate yesterday,
            OvertakeMode mode
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
            OvertakeMode mode,
            LocalDate serviceDate,
            Integer weekdayIso
    ) throws Exception {
        boolean summary = serviceDate == null;
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(mode.selectColumns(summary));
        sql.append(" FROM read_parquet(?) ");
        if (serviceDate != null) {
            sql.append("WHERE CAST(serviceDate AS VARCHAR) = ? ");
        } else if (weekdayIso != null) {
            sql.append("WHERE weekdayIso = ? ");
        }
        sql.append("ORDER BY ");
        sql.append(summary ? "totalOvertakes DESC, averageDailyOvertakes DESC" : "overtakeCount DESC, latestOvertakeAt DESC");

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
                    rows.add(mode.readRow(rs, summary));
                }
                return rows;
            }
        }
    }

    private static Map<String, List<Map<String, Object>>> loadVehicleDetails(Connection connection, Path overtakeEventsDir) throws Exception {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        if (!Files.exists(overtakeEventsDir)) {
            return result;
        }

        String sql = "SELECT "
                + "CAST(serviceDate AS VARCHAR), weekdayIso, overtakerPlate, overtakerRouteNumber, "
                + "overtakenPlate, routeNumber, segmentId, startStopName, endStopName, "
                + "startStopLatitude, startStopLongitude, endStopLatitude, endStopLongitude, distanceMeters, "
                + "overtakeAt, overtakerTravelDurationSeconds, overtakenTravelDurationSeconds, "
                + "overtakerAvgSegmentSpeedKmh, overtakenAvgSegmentSpeedKmh "
                + "FROM read_parquet(?) "
                + "ORDER BY overtakeAt DESC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob(overtakeEventsDir));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String overtakerPlate = rs.getString(3);
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("serviceDate", rs.getString(1));
                    row.put("weekdayIso", rs.getInt(2));
                    row.put("overtakerPlate", overtakerPlate);
                    row.put("overtakerRouteNumber", rs.getString(4));
                    row.put("overtakenPlate", rs.getString(5));
                    row.put("overtakenRouteNumber", rs.getString(6));
                    row.put("segmentId", rs.getString(7));
                    row.put("startStopName", rs.getString(8));
                    row.put("endStopName", rs.getString(9));
                    row.put("startStopLatitude", rs.getDouble(10));
                    row.put("startStopLongitude", rs.getDouble(11));
                    row.put("endStopLatitude", rs.getDouble(12));
                    row.put("endStopLongitude", rs.getDouble(13));
                    row.put("distanceMeters", rs.getDouble(14));
                    row.put("overtakeAt", toIsoString(rs.getObject(15)));
                    row.put("overtakerTravelDurationSeconds", rs.getLong(16));
                    row.put("overtakenTravelDurationSeconds", rs.getLong(17));
                    row.put("overtakerAvgSegmentSpeedKmh", rs.getDouble(18));
                    row.put("overtakenAvgSegmentSpeedKmh", rs.getDouble(19));
                    result.computeIfAbsent(overtakerPlate, ignored -> new ArrayList<>()).add(row);
                }
            }
        }
        return result;
    }

    private static Map<String, List<Map<String, Object>>> loadPhysicalVehicleDetails(Connection connection, Path overtakeEventsDir) throws Exception {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        if (!Files.exists(overtakeEventsDir)) {
            return result;
        }

        String sql = "SELECT "
                + "CAST(serviceDate AS VARCHAR), weekdayIso, overtakerPlate, overtakerRouteNumber, "
                + "overtakenPlate, overtakenRouteNumber, physicalSegmentId, startStopName, endStopName, "
                + "startStopLatitude, startStopLongitude, endStopLatitude, endStopLongitude, distanceMeters, "
                + "overtakeAt, overtakerTravelDurationSeconds, overtakenTravelDurationSeconds, "
                + "overtakerAvgSegmentSpeedKmh, overtakenAvgSegmentSpeedKmh "
                + "FROM read_parquet(?) "
                + "ORDER BY overtakeAt DESC";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, parquetGlob(overtakeEventsDir));
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String overtakerPlate = rs.getString(3);
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("serviceDate", rs.getString(1));
                    row.put("weekdayIso", rs.getInt(2));
                    row.put("overtakerPlate", overtakerPlate);
                    row.put("overtakerRouteNumber", rs.getString(4));
                    row.put("overtakenPlate", rs.getString(5));
                    row.put("overtakenRouteNumber", rs.getString(6));
                    row.put("segmentId", rs.getString(7));
                    row.put("physicalSegmentId", rs.getString(7));
                    row.put("startStopName", rs.getString(8));
                    row.put("endStopName", rs.getString(9));
                    row.put("startStopLatitude", rs.getDouble(10));
                    row.put("startStopLongitude", rs.getDouble(11));
                    row.put("endStopLatitude", rs.getDouble(12));
                    row.put("endStopLongitude", rs.getDouble(13));
                    row.put("distanceMeters", rs.getDouble(14));
                    row.put("overtakeAt", toIsoString(rs.getObject(15)));
                    row.put("overtakerTravelDurationSeconds", rs.getLong(16));
                    row.put("overtakenTravelDurationSeconds", rs.getLong(17));
                    row.put("overtakerAvgSegmentSpeedKmh", rs.getDouble(18));
                    row.put("overtakenAvgSegmentSpeedKmh", rs.getDouble(19));
                    result.computeIfAbsent(overtakerPlate, ignored -> new ArrayList<>()).add(row);
                }
            }
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
        payload.put("routeShapes", List.of());
        Map<String, Object> emptySection = Map.of(
                "previousDay", List.of(),
                "allDays", List.of(),
                "weekday", Map.of()
        );
        payload.put("segmentData", emptySection);
        payload.put("segmentRouteData", emptySection);
        payload.put("segmentVehicleData", emptySection);
        payload.put("vehicleData", emptySection);
        payload.put("vehicleDetails", Map.of());
        payload.put("pointHeatmapData", emptySection);
        payload.put("physicalSegmentData", emptySection);
        payload.put("physicalSegmentVehicleData", emptySection);
        payload.put("physicalVehicleData", emptySection);
        payload.put("physicalVehicleDetails", Map.of());
        payload.put("physicalPointHeatmapData", emptySection);
        return payload;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws Exception {
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private enum OvertakeMode {
        SEGMENT(
                "segmentId, routeNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, latestOvertakerPlate, latestOvertakerRouteNumber, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds",
                "segmentId, routeNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt"
        ),
        SEGMENT_ROUTE(
                "segmentId, routeNumber, overtakerRouteNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, latestOvertakerPlate, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds",
                "segmentId, routeNumber, overtakerRouteNumber, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt, latestOvertakerPlate"
        ),
        SEGMENT_VEHICLE(
                "segmentId, routeNumber, overtakerRouteNumber, overtakerPlate, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, overtakeCount, latestOvertakeAt, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds",
                "segmentId, routeNumber, overtakerRouteNumber, overtakerPlate, startStopId, startStopName, startStopLatitude, startStopLongitude, endStopId, endStopName, endStopLatitude, endStopLongitude, distanceMeters, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt"
        ),
        VEHICLE(
                "overtakerRouteNumber, overtakerPlate, overtakeCount, latestOvertakeAt, latestStartStopName, latestEndStopName, averageOvertakerDurationSeconds, averageOvertakenDurationSeconds",
                "overtakerRouteNumber, overtakerPlate, totalOvertakes, sampleDays, averageDailyOvertakes, maxDailyOvertakes, latestOvertakeAt, latestStartStopName, latestEndStopName"
        );

        private final String dailySelectColumns;
        private final String summarySelectColumns;

        OvertakeMode(String dailySelectColumns, String summarySelectColumns) {
            this.dailySelectColumns = dailySelectColumns;
            this.summarySelectColumns = summarySelectColumns;
        }

        String selectColumns(boolean summary) {
            return summary ? summarySelectColumns : dailySelectColumns;
        }

        Map<String, Object> readRow(ResultSet rs, boolean summary) throws Exception {
            switch (this) {
                case SEGMENT:
                    return readSegmentRow(rs, summary);
                case SEGMENT_ROUTE:
                    return readSegmentRouteRow(rs, summary);
                case SEGMENT_VEHICLE:
                    return readSegmentVehicleRow(rs, summary);
                case VEHICLE:
                    return readVehicleRow(rs, summary);
                default:
                    throw new IllegalStateException("Unexpected value: " + this);
            }
        }

        private static Map<String, Object> readSegmentRow(ResultSet rs, boolean summary) throws Exception {
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
                row.put("latestOvertakerPlate", rs.getString(14));
                row.put("latestOvertakerRouteNumber", rs.getString(15));
                row.put("averageOvertakerDurationSeconds", rs.getInt(16));
                row.put("averageOvertakenDurationSeconds", rs.getInt(17));
            }
            return row;
        }

        private static Map<String, Object> readSegmentRouteRow(ResultSet rs, boolean summary) throws Exception {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("segmentId", rs.getString(1));
            row.put("routeNumber", rs.getString(2));
            row.put("overtakerRouteNumber", rs.getString(3));
            row.put("startStopId", rs.getString(4));
            row.put("startStopName", rs.getString(5));
            row.put("startStopLatitude", rs.getDouble(6));
            row.put("startStopLongitude", rs.getDouble(7));
            row.put("endStopId", rs.getString(8));
            row.put("endStopName", rs.getString(9));
            row.put("endStopLatitude", rs.getDouble(10));
            row.put("endStopLongitude", rs.getDouble(11));
            row.put("distanceMeters", rs.getDouble(12));
            if (summary) {
                row.put("totalOvertakes", rs.getLong(13));
                row.put("sampleDays", rs.getLong(14));
                row.put("averageDailyOvertakes", rs.getDouble(15));
                row.put("maxDailyOvertakes", rs.getLong(16));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(17)));
                row.put("latestOvertakerPlate", rs.getString(18));
            } else {
                row.put("overtakeCount", rs.getLong(13));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(14)));
                row.put("latestOvertakerPlate", rs.getString(15));
                row.put("averageOvertakerDurationSeconds", rs.getInt(16));
                row.put("averageOvertakenDurationSeconds", rs.getInt(17));
            }
            return row;
        }

        private static Map<String, Object> readSegmentVehicleRow(ResultSet rs, boolean summary) throws Exception {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("segmentId", rs.getString(1));
            row.put("routeNumber", rs.getString(2));
            row.put("overtakerRouteNumber", rs.getString(3));
            row.put("overtakerPlate", rs.getString(4));
            row.put("startStopId", rs.getString(5));
            row.put("startStopName", rs.getString(6));
            row.put("startStopLatitude", rs.getDouble(7));
            row.put("startStopLongitude", rs.getDouble(8));
            row.put("endStopId", rs.getString(9));
            row.put("endStopName", rs.getString(10));
            row.put("endStopLatitude", rs.getDouble(11));
            row.put("endStopLongitude", rs.getDouble(12));
            row.put("distanceMeters", rs.getDouble(13));
            if (summary) {
                row.put("totalOvertakes", rs.getLong(14));
                row.put("sampleDays", rs.getLong(15));
                row.put("averageDailyOvertakes", rs.getDouble(16));
                row.put("maxDailyOvertakes", rs.getLong(17));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(18)));
            } else {
                row.put("overtakeCount", rs.getLong(14));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(15)));
                row.put("averageOvertakerDurationSeconds", rs.getInt(16));
                row.put("averageOvertakenDurationSeconds", rs.getInt(17));
            }
            return row;
        }

        private static Map<String, Object> readVehicleRow(ResultSet rs, boolean summary) throws Exception {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("overtakerRouteNumber", rs.getString(1));
            row.put("overtakerPlate", rs.getString(2));
            if (summary) {
                row.put("totalOvertakes", rs.getLong(3));
                row.put("sampleDays", rs.getLong(4));
                row.put("averageDailyOvertakes", rs.getDouble(5));
                row.put("maxDailyOvertakes", rs.getLong(6));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(7)));
                row.put("latestStartStopName", rs.getString(8));
                row.put("latestEndStopName", rs.getString(9));
            } else {
                row.put("overtakeCount", rs.getLong(3));
                row.put("latestOvertakeAt", toIsoString(rs.getObject(4)));
                row.put("latestStartStopName", rs.getString(5));
                row.put("latestEndStopName", rs.getString(6));
                row.put("averageOvertakerDurationSeconds", rs.getInt(7));
                row.put("averageOvertakenDurationSeconds", rs.getInt(8));
            }
            return row;
        }
    }
}
