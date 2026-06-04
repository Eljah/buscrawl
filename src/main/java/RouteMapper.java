import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RouteMapper {
    private static final int ROUTE_NAME_INDEX = 1;
    private static final int TRANSPORT_TYPE_INDEX = 5;
    private static final int TTYPE_TROLLEYBUS = 1;
    private static final int TTYPE_TRAM = 2;
    private final JSONObject routeData;

    public RouteMapper(String filePath) {
        try {
            String content;
            if (filePath == null || filePath.isBlank()) {
                try (InputStream inputStream = RouteMapper.class.getClassLoader().getResourceAsStream("routes.json")) {
                    if (inputStream == null) {
                        throw new IllegalStateException("routes.json not found in classpath");
                    }
                    content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                content = Files.readString(Paths.get(filePath));
            }
            routeData = new JSONObject(content).getJSONObject("bus");
        } catch (Exception e) {
            throw new RuntimeException("Ошибка загрузки данных маршрутов: " + e.getMessage(), e);
        }
    }

    public String getRealRouteNumber(String internalRouteId) {
        return getDisplayRouteNumber(internalRouteId, internalRouteId);
    }

    public String getDisplayRouteNumber(String internalRouteId, String fallbackRouteNumber) {
        if (routeData.has(internalRouteId)) {
            JSONArray entry = routeData.optJSONArray(internalRouteId);
            if (entry != null && entry.length() > ROUTE_NAME_INDEX) {
                String baseRouteNumber = entry.optString(ROUTE_NAME_INDEX, fallbackRouteNumber);
                int transportType = entry.optInt(TRANSPORT_TYPE_INDEX, 0);
                return formatRouteNumber(baseRouteNumber, transportType);
            }
        }
        return fallbackRouteNumber;
    }

    public List<String> getAllDisplayRouteNumbers() {
        List<String> routeNumbers = new ArrayList<>();
        for (String internalRouteId : routeData.keySet()) {
            routeNumbers.add(getDisplayRouteNumber(internalRouteId, internalRouteId));
        }
        routeNumbers.sort(Comparator.comparing(RouteMapper::routeSortKey));

        List<String> uniqueRoutes = new ArrayList<>();
        String previous = null;
        for (String routeNumber : routeNumbers) {
            if (!routeNumber.equals(previous)) {
                uniqueRoutes.add(routeNumber);
                previous = routeNumber;
            }
        }
        return uniqueRoutes;
    }

    public static String formatRouteNumber(String baseRouteNumber, int transportType) {
        if (baseRouteNumber == null) {
            return null;
        }

        if (transportType == TTYPE_TROLLEYBUS || transportType == TTYPE_TRAM) {
            return baseRouteNumber.startsWith("Т") ? baseRouteNumber : "Т" + baseRouteNumber;
        }
        return baseRouteNumber;
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
}
