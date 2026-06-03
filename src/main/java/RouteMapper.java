import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RouteMapper {
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
        if (routeData.has(internalRouteId)) {
            JSONArray entry = routeData.optJSONArray(internalRouteId);
            if (entry != null && entry.length() > 1) {
                return entry.optString(1, internalRouteId);
            }
        }
        return internalRouteId;
    }
}
