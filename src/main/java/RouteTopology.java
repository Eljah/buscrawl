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

    private RouteTopology(
            Map<String, StopInfo> stopsById,
            Map<String, RouteInfo> routesById,
            List<RouteStopInfo> routeStops
    ) {
        this.stopsById = stopsById;
        this.routesById = routesById;
        this.routeStops = routeStops;
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

            return new RouteTopology(stopsById, routesById, routeStops);
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

    public Collection<RouteInfo> getRoutes() {
        return routesById.values();
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
}
