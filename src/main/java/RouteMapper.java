import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RouteMapper {

    private final Map<String, String> routeIdToNumber = new HashMap<>();

    public RouteMapper() {
        loadRoutes();
    }

    private void loadRoutes() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("routes.json")) {
            if (inputStream == null) {
                throw new RuntimeException("❌ routes.json not found in resources!");
            }

            String jsonContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            JSONObject json = new JSONObject(jsonContent);
            JSONObject buses = json.getJSONObject("bus");

            for (String internalId : buses.keySet()) {
                JSONArray busData = buses.getJSONArray(internalId);
                String routeNumber = busData.getString(1);
                routeIdToNumber.put(internalId, routeNumber);
            }

            System.out.println("✅ Loaded routes: " + routeIdToNumber.size());
        } catch (Exception e) {
            throw new RuntimeException("❌ Error loading routes: " + e.getMessage(), e);
        }
    }

    /**
     * Получение реального номера маршрута по внутреннему идентификатору (например, "970").
     */
    public String getRouteNumberByInternalId(String internalId) {
        return routeIdToNumber.getOrDefault(internalId, "Unknown");
    }
}
