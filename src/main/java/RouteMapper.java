import org.json.JSONArray;
import org.json.JSONObject;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RouteMapper {
    private JSONObject routeData;

    public RouteMapper(String filePath) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            routeData = new JSONObject(content).getJSONObject("bus");
        } catch (Exception e) {
            throw new RuntimeException("Ошибка загрузки данных маршрутов: " + e.getMessage());
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
