import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class RouteTopology {
    private static final int ROUTE_NAME_INDEX = 1;
    private static final int TRANSPORT_TYPE_INDEX = 5;
    private static final int STOP_NAME_INDEX = 0;
    private static final int STOP_LONGITUDE_INDEX = 1;
    private static final int STOP_LATITUDE_INDEX = 2;

    private final Map<String, StopInfo> stopsById;
    private final Map<String, RouteInfo> routesById;
    private final List<RouteStopInfo> routeStops;
    private final List<SegmentInfo> adjacentSegments;

    private RouteTopology(
            Map<String, StopInfo> stopsById,
            Map<String, RouteInfo> routesById,
            List<RouteStopInfo> routeStops,
            List<SegmentInfo> adjacentSegments
    ) {
        this.stopsById = stopsById;
        this.routesById = routesById;
        this.routeStops = routeStops;
        this.adjacentSegments = adjacentSegments;
    }

    public static RouteTopology load(String filePath) {
        try {
            String content;
            if (filePath == null || filePath.isBlank()) {
                try (InputStream inputStream = RouteTopology.class.getClassLoader().getResourceAsStream("routes.json")) {
                    if (inputStream == null) {
                        throw new IllegalStateException("routes.json not found in classpath");
                    }
                    content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                content = Files.readString(Path.of(filePath));
            }

            JSONObject root = new JSONObject(content);
            JSONObject stopsJson = root.getJSONObject("nbusstop");
            JSONObject busJson = root.getJSONObject("bus");
            JSONObject routeJson = root.getJSONObject("route");

            Map<String, StopInfo> stopsById = new LinkedHashMap<>();
            for (String stopId : stopsJson.keySet()) {
                JSONArray stopEntry = stopsJson.getJSONArray(stopId);
                stopsById.put(stopId, new StopInfo(
                        stopId,
                        stopEntry.optString(STOP_NAME_INDEX, stopId),
                        stopEntry.optDouble(STOP_LATITUDE_INDEX),
                        stopEntry.optDouble(STOP_LONGITUDE_INDEX)
                ));
            }

            Map<String, RouteInfo> routesById = new LinkedHashMap<>();
            for (String internalRouteId : busJson.keySet()) {
                JSONArray routeEntry = busJson.getJSONArray(internalRouteId);
                String baseRouteNumber = routeEntry.optString(ROUTE_NAME_INDEX, internalRouteId);
                int transportType = routeEntry.optInt(TRANSPORT_TYPE_INDEX, 0);
                String displayRouteNumber = RouteMapper.formatRouteNumber(baseRouteNumber, transportType);
                routesById.put(internalRouteId, new RouteInfo(
                        internalRouteId,
                        baseRouteNumber,
                        displayRouteNumber,
                        transportType
                ));
            }

            List<RouteStopInfo> routeStops = new ArrayList<>();
            for (String routeStopKey : routeJson.keySet()) {
                JSONArray routeStopEntry = routeJson.getJSONArray(routeStopKey);
                String internalRouteId = String.valueOf(routeStopEntry.getInt(0));
                String stopId = String.valueOf(routeStopEntry.getInt(1));
                int direction = routeStopEntry.getInt(2);
                int stopOrder = routeStopEntry.getInt(3);
                RouteInfo routeInfo = routesById.get(internalRouteId);
                StopInfo stopInfo = stopsById.get(stopId);
                if (routeInfo == null || stopInfo == null) {
                    continue;
                }
                routeStops.add(new RouteStopInfo(
                        routeInfo.internalRouteId,
                        routeInfo.displayRouteNumber,
                        stopInfo.stopId,
                        stopInfo.stopName,
                        stopInfo.latitude,
                        stopInfo.longitude,
                        direction,
                        stopOrder
                ));
            }

            routeStops.sort(Comparator
                    .comparing((RouteStopInfo info) -> info.internalRouteId)
                    .thenComparingInt(info -> info.direction)
                    .thenComparingInt(info -> info.stopOrder));

            List<SegmentInfo> adjacentSegments = new ArrayList<>();
            Map<String, List<RouteStopInfo>> stopsByRouteDirection = routeStops.stream()
                    .collect(Collectors.groupingBy(
                            routeStop -> routeStop.internalRouteId + "|" + routeStop.direction,
                            LinkedHashMap::new,
                            Collectors.toList()
                    ));
            for (List<RouteStopInfo> routeDirectionStops : stopsByRouteDirection.values()) {
                routeDirectionStops.sort(Comparator.comparingInt(RouteStopInfo::getStopOrder));
                for (int i = 1; i < routeDirectionStops.size(); i++) {
                    RouteStopInfo start = routeDirectionStops.get(i - 1);
                    RouteStopInfo end = routeDirectionStops.get(i);
                    if (end.stopOrder != start.stopOrder + 1) {
                        continue;
                    }
                    adjacentSegments.add(new SegmentInfo(
                            start.internalRouteId,
                            start.routeNumber,
                            start.direction,
                            start.stopOrder,
                            end.stopOrder,
                            start.stopId,
                            start.stopName,
                            start.stopLatitude,
                            start.stopLongitude,
                            end.stopId,
                            end.stopName,
                            end.stopLatitude,
                            end.stopLongitude,
                            haversineMeters(
                                    start.stopLatitude,
                                    start.stopLongitude,
                                    end.stopLatitude,
                                    end.stopLongitude
                            )
                    ));
                }
            }

            return new RouteTopology(stopsById, routesById, routeStops, adjacentSegments);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load route topology", e);
        }
    }

    public Dataset<Row> createRouteMetadataDataFrame(SparkSession spark) {
        List<Row> rows = routesById.values().stream()
                .map(route -> RowFactory.create(
                        route.internalRouteId,
                        route.baseRouteNumber,
                        route.displayRouteNumber,
                        route.transportType
                ))
                .collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("internalRouteId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("baseRouteNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("routeNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transportType", DataTypes.IntegerType, false, Metadata.empty())
        });

        return spark.createDataFrame(rows, schema);
    }

    public Dataset<Row> createRouteStopPointsDataFrame(SparkSession spark) {
        List<Row> rows = routeStops.stream()
                .map(routeStop -> RowFactory.create(
                        routeStop.internalRouteId,
                        routeStop.routeNumber,
                        routeStop.stopId,
                        routeStop.stopName,
                        routeStop.stopLatitude,
                        routeStop.stopLongitude,
                        routeStop.direction,
                        routeStop.stopOrder
                ))
                .collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("internalRouteId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("routeNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("stopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("stopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("stopLatitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("stopLongitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("direction", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("stopOrder", DataTypes.IntegerType, false, Metadata.empty())
        });

        return spark.createDataFrame(rows, schema);
    }

    public Dataset<Row> createAdjacentSegmentsDataFrame(SparkSession spark) {
        List<Row> rows = adjacentSegments.stream()
                .map(segment -> RowFactory.create(
                        segment.segmentId,
                        segment.internalRouteId,
                        segment.routeNumber,
                        segment.direction,
                        segment.startStopOrder,
                        segment.endStopOrder,
                        segment.startStopId,
                        segment.startStopName,
                        segment.startStopLatitude,
                        segment.startStopLongitude,
                        segment.endStopId,
                        segment.endStopName,
                        segment.endStopLatitude,
                        segment.endStopLongitude,
                        segment.distanceMeters
                ))
                .collect(Collectors.toList());

        StructType schema = new StructType(new StructField[]{
                new StructField("segmentId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("internalRouteId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("routeNumber", DataTypes.StringType, false, Metadata.empty()),
                new StructField("direction", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("startStopOrder", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("endStopOrder", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("startStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("startStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("startStopLatitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("startStopLongitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("endStopId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("endStopName", DataTypes.StringType, false, Metadata.empty()),
                new StructField("endStopLatitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("endStopLongitude", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("distanceMeters", DataTypes.DoubleType, false, Metadata.empty())
        });

        return spark.createDataFrame(rows, schema);
    }

    public List<Map<String, Object>> buildRouteShapes() {
        Map<String, List<RouteStopInfo>> byRouteDirection = routeStops.stream()
                .collect(Collectors.groupingBy(
                        routeStop -> routeStop.routeNumber + "|" + routeStop.internalRouteId + "|" + routeStop.direction,
                        LinkedHashMap::new,
                        Collectors.toList()
                ));

        Map<String, List<List<Map<String, Object>>>> shapesByRoute = new LinkedHashMap<>();
        byRouteDirection.values().forEach(stops -> {
            if (stops.isEmpty()) {
                return;
            }
            stops.sort(Comparator.comparingInt(RouteStopInfo::getStopOrder));
            String routeNumber = stops.get(0).routeNumber;
            List<Map<String, Object>> path = new ArrayList<>();
            for (RouteStopInfo routeStop : stops) {
                Map<String, Object> point = new LinkedHashMap<>();
                point.put("latitude", routeStop.stopLatitude);
                point.put("longitude", routeStop.stopLongitude);
                path.add(point);
            }
            shapesByRoute.computeIfAbsent(routeNumber, ignored -> new ArrayList<>()).add(path);
        });

        return shapesByRoute.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(RouteTopology::routeSortKey)))
                .map(entry -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("routeNumber", entry.getKey());
                    item.put("paths", entry.getValue());
                    return item;
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> buildStops() {
        return stopsById.values().stream()
                .sorted(Comparator.comparing(stop -> stop.stopName.toLowerCase(Locale.ROOT)))
                .map(stop -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("stopId", stop.stopId);
                    item.put("stopName", stop.stopName);
                    item.put("latitude", stop.latitude);
                    item.put("longitude", stop.longitude);
                    return item;
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> buildSegmentShapes() {
        return adjacentSegments.stream()
                .map(segment -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("segmentId", segment.segmentId);
                    item.put("routeNumber", segment.routeNumber);
                    item.put("direction", segment.direction);
                    item.put("startStopId", segment.startStopId);
                    item.put("startStopName", segment.startStopName);
                    item.put("endStopId", segment.endStopId);
                    item.put("endStopName", segment.endStopName);
                    item.put("distanceMeters", segment.distanceMeters);
                    List<Map<String, Object>> path = new ArrayList<>();
                    Map<String, Object> start = new LinkedHashMap<>();
                    start.put("latitude", segment.startStopLatitude);
                    start.put("longitude", segment.startStopLongitude);
                    path.add(start);
                    Map<String, Object> end = new LinkedHashMap<>();
                    end.put("latitude", segment.endStopLatitude);
                    end.put("longitude", segment.endStopLongitude);
                    path.add(end);
                    item.put("path", path);
                    return item;
                })
                .collect(Collectors.toList());
    }

    public Collection<RouteInfo> getRoutes() {
        return routesById.values();
    }

    public Collection<SegmentInfo> getAdjacentSegments() {
        return adjacentSegments;
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double earthRadiusMeters = 6_371_000;
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2)
                * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        return 2 * earthRadiusMeters * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    private static String routeSortKey(String routeNumber) {
        String normalized = routeNumber == null ? "" : routeNumber;
        boolean electric = normalized.startsWith("Т");
        String withoutPrefix = electric ? normalized.substring(1) : normalized;
        String digits = withoutPrefix.replaceAll("[^0-9]", "");
        String suffix = withoutPrefix.replaceAll("[0-9]", "");
        String numberPart = digits.isEmpty() ? "9999" : String.format("%04d", Integer.parseInt(digits));
        return numberPart + ":" + (electric ? "Т" : "") + ":" + suffix + ":" + normalized;
    }

    public static final class RouteInfo {
        private final String internalRouteId;
        private final String baseRouteNumber;
        private final String displayRouteNumber;
        private final int transportType;

        private RouteInfo(String internalRouteId, String baseRouteNumber, String displayRouteNumber, int transportType) {
            this.internalRouteId = internalRouteId;
            this.baseRouteNumber = baseRouteNumber;
            this.displayRouteNumber = displayRouteNumber;
            this.transportType = transportType;
        }
    }

    public static final class StopInfo {
        private final String stopId;
        private final String stopName;
        private final double latitude;
        private final double longitude;

        private StopInfo(String stopId, String stopName, double latitude, double longitude) {
            this.stopId = stopId;
            this.stopName = stopName;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    public static final class RouteStopInfo {
        private final String internalRouteId;
        private final String routeNumber;
        private final String stopId;
        private final String stopName;
        private final double stopLatitude;
        private final double stopLongitude;
        private final int direction;
        private final int stopOrder;

        private RouteStopInfo(
                String internalRouteId,
                String routeNumber,
                String stopId,
                String stopName,
                double stopLatitude,
                double stopLongitude,
                int direction,
                int stopOrder
        ) {
            this.internalRouteId = internalRouteId;
            this.routeNumber = routeNumber;
            this.stopId = stopId;
            this.stopName = stopName;
            this.stopLatitude = stopLatitude;
            this.stopLongitude = stopLongitude;
            this.direction = direction;
            this.stopOrder = stopOrder;
        }

        public int getStopOrder() {
            return stopOrder;
        }
    }

    public static final class SegmentInfo {
        private final String segmentId;
        private final String internalRouteId;
        private final String routeNumber;
        private final int direction;
        private final int startStopOrder;
        private final int endStopOrder;
        private final String startStopId;
        private final String startStopName;
        private final double startStopLatitude;
        private final double startStopLongitude;
        private final String endStopId;
        private final String endStopName;
        private final double endStopLatitude;
        private final double endStopLongitude;
        private final double distanceMeters;

        private SegmentInfo(
                String internalRouteId,
                String routeNumber,
                int direction,
                int startStopOrder,
                int endStopOrder,
                String startStopId,
                String startStopName,
                double startStopLatitude,
                double startStopLongitude,
                String endStopId,
                String endStopName,
                double endStopLatitude,
                double endStopLongitude,
                double distanceMeters
        ) {
            this.segmentId = internalRouteId + "|" + direction + "|" + startStopId + "|" + endStopId;
            this.internalRouteId = internalRouteId;
            this.routeNumber = routeNumber;
            this.direction = direction;
            this.startStopOrder = startStopOrder;
            this.endStopOrder = endStopOrder;
            this.startStopId = startStopId;
            this.startStopName = startStopName;
            this.startStopLatitude = startStopLatitude;
            this.startStopLongitude = startStopLongitude;
            this.endStopId = endStopId;
            this.endStopName = endStopName;
            this.endStopLatitude = endStopLatitude;
            this.endStopLongitude = endStopLongitude;
            this.distanceMeters = distanceMeters;
        }
    }
}
