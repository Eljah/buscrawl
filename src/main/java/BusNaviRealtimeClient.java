import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public class BusNaviRealtimeClient {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault(
            "BUS_CITY_TIMEZONE",
            "Europe/Moscow"
    ));
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd HH:mm:ss",
            Locale.ROOT
    );

    private final OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(Duration.ofSeconds(15))
            .readTimeout(Duration.ofSeconds(30))
            .callTimeout(Duration.ofSeconds(45))
            .build();
    private final String apiBaseUrl;
    private final String sysId;
    private final NaviRouteMap routeMap;
    private long requestId = 1L;
    private long lastTimestampSec = 0L;
    private String sid;

    public BusNaviRealtimeClient(String apiBaseUrl, String sysId, NaviRouteMap routeMap) {
        this.apiBaseUrl = apiBaseUrl.replaceFirst("/+$", "");
        this.sysId = sysId;
        this.routeMap = routeMap;
    }

    public static void main(String[] args) throws Exception {
        Path storageRoot = Paths.get(System.getenv().getOrDefault("BUS_STORAGE_ROOT", "./var/bus"));
        BusRawJsonSpool rawSpool = BusRawJsonSpool.fromEnvironment(storageRoot);
        String routeMapFile = System.getenv().getOrDefault("BUS_NAVI_ROUTE_MAP_FILE", "");
        NaviRouteMap routeMap = NaviRouteMap.load(routeMapFile);
        BusNaviRealtimeClient client = new BusNaviRealtimeClient(
                System.getenv().getOrDefault("BUS_NAVI_BASE_URL", "https://navi.kazantransport.ru/api"),
                System.getenv().getOrDefault("BUS_NAVI_SYS_ID", "kazan"),
                routeMap
        );

        double minLat = Double.parseDouble(System.getenv().getOrDefault("BUS_NAVI_MIN_LAT", "55.55"));
        double maxLat = Double.parseDouble(System.getenv().getOrDefault("BUS_NAVI_MAX_LAT", "56.05"));
        double minLon = Double.parseDouble(System.getenv().getOrDefault("BUS_NAVI_MIN_LON", "48.75"));
        double maxLon = Double.parseDouble(System.getenv().getOrDefault("BUS_NAVI_MAX_LON", "49.55"));
        long pollMillis = Math.max(1_000L, Long.parseLong(System.getenv().getOrDefault(
                "BUS_NAVI_POLL_SECONDS",
                "15"
        )) * 1000L);
        long maxSourceLagSeconds = Long.parseLong(System.getenv().getOrDefault(
                "BUS_NAVI_MAX_SOURCE_LAG_SECONDS",
                "900"
        ));

        System.out.println("Navi raw client started");
        System.out.println("Raw bus events spool: " + rawSpool.getRootDir().toAbsolutePath());
        System.out.printf(
                Locale.ROOT,
                "Navi bbox: minLat=%.6f maxLat=%.6f minLon=%.6f maxLon=%.6f poll=%dms mappedRoutes=%d%n",
                minLat,
                maxLat,
                minLon,
                maxLon,
                pollMillis,
                routeMap.size()
        );

        long pollCount = 0L;
        while (true) {
            long started = System.currentTimeMillis();
            try {
                if (client.sid == null || client.sid.isBlank()) {
                    client.startSession();
                }
                PollStats stats = client.pollUnits(rawSpool, minLat, maxLat, minLon, maxLon, maxSourceLagSeconds);
                pollCount++;
                if (pollCount == 1L || pollCount % 20L == 0L || stats.unknownRoutes > 0L || stats.errors > 0L) {
                    System.out.printf(
                            Locale.ROOT,
                            "Navi poll #%d: units=%d written=%d skippedUnknownRoute=%d skippedStale=%d errors=%d sid=%s%n",
                            pollCount,
                            stats.units,
                            stats.written,
                            stats.unknownRoutes,
                            stats.stale,
                            stats.errors,
                            client.sid
                    );
                }
            } catch (Exception e) {
                client.sid = null;
                System.err.println("Navi poll failed, session will be recreated: " + e.getMessage());
                e.printStackTrace(System.err);
            }
            long elapsed = System.currentTimeMillis() - started;
            Thread.sleep(Math.max(1_000L, pollMillis - elapsed));
        }
    }

    private void startSession() throws Exception {
        JSONObject result = rpc("startSession", new JSONObject(), false);
        sid = result.getString("sid");
        System.out.println("Navi session started: " + sid);
    }

    private PollStats pollUnits(
            BusRawJsonSpool rawSpool,
            double minLat,
            double maxLat,
            double minLon,
            double maxLon,
            long maxSourceLagSeconds
    ) throws Exception {
        JSONObject params = new JSONObject()
                .put("sid", sid)
                .put("minlat", minLat)
                .put("maxlat", maxLat)
                .put("minlong", minLon)
                .put("maxlong", maxLon);
        JSONArray units = rpcArray("getUnitsInRect", params);
        PollStats stats = new PollStats();
        stats.units = units.length();
        Instant observedAt = Instant.now();

        for (int i = 0; i < units.length(); i++) {
            try {
                JSONObject unit = units.getJSONObject(i);
                NaviRouteMap.Route route = routeMap.byNaviMrId(unit.optString("mr_id", ""));
                if (route == null) {
                    stats.unknownRoutes++;
                    continue;
                }
                Instant sourceTime = parseSourceTime(unit.optString("u_timenav", ""), observedAt);
                if (Math.abs(observedAt.getEpochSecond() - sourceTime.getEpochSecond()) > maxSourceLagSeconds) {
                    stats.stale++;
                    continue;
                }
                String plate = firstNonBlank(
                        unit.optString("u_statenum", ""),
                        unit.optString("u_garagnum", ""),
                        unit.optString("u_id", "")
                );
                if (plate == null) {
                    stats.errors++;
                    continue;
                }

                JSONObject row = new JSONObject()
                        .put("source", "navi")
                        .put("naviUnitId", unit.optString("u_id", ""))
                        .put("naviRouteId", route.naviMrId)
                        .put("naviTransportTypeId", route.naviTransportTypeId)
                        .put("internalRouteId", route.internalRouteId)
                        .put("realRouteNumber", route.displayRouteNumber)
                        .put("latitude", parseDouble(unit.optString("u_lat", "")))
                        .put("longitude", parseDouble(unit.optString("u_long", "")))
                        .put("speed", parseInt(unit.optString("u_speed", "0")))
                        .put("course", parseInt(unit.optString("u_course", "0")))
                        .put("plate", plate)
                        .put("timestamp", observedAt.getEpochSecond())
                        .put("readableTime", DATE_FORMAT.format(observedAt.atZone(CITY_ZONE)))
                        .put("sourceTimestamp", sourceTime.getEpochSecond())
                        .put("sourceReadableTime", DATE_FORMAT.format(sourceTime.atZone(CITY_ZONE)));

                rawSpool.append(row.toString());
                stats.written++;
            } catch (Exception e) {
                stats.errors++;
                System.err.println("Failed to convert Navi unit: " + e.getMessage());
            }
        }
        return stats;
    }

    private JSONObject rpc(String method, JSONObject params, boolean signed) throws Exception {
        JSONObject payload = rpcRaw(method, params, signed);
        return payload.getJSONObject("result");
    }

    private JSONArray rpcArray(String method, JSONObject params) throws Exception {
        JSONObject payload = rpcRaw(method, params, true);
        return payload.getJSONArray("result");
    }

    private JSONObject rpcRaw(String method, JSONObject params, boolean signed) throws Exception {
        long id = nextRequestId();
        JSONObject requestParams = new JSONObject(params.toString());
        JSONObject request = new JSONObject()
                .put("jsonrpc", "2\u20242")
                .put("method", method)
                .put("ts", nextTimestampSec())
                .put("params", requestParams)
                .put("id", id);
        String url = apiBaseUrl + "/rpc.php";
        if (signed) {
            Signature signature = sign(method, id, requestParams.optString("sid", ""));
            requestParams.put("magic", signature.magic);
            url += "?m=" + signature.guid;
        }

        Request httpRequest = new Request.Builder()
                .url(url)
                .header("Origin", "https://navi.kazantransport.ru")
                .header("Referer", "https://navi.kazantransport.ru/index.html")
                .header("User-Agent", "Mozilla/5.0")
                .post(RequestBody.create(request.toString(), JSON))
                .build();
        try (Response response = httpClient.newCall(httpRequest).execute()) {
            String body = response.body() == null ? "" : response.body().string();
            if (!response.isSuccessful()) {
                throw new IllegalStateException(method + " HTTP " + response.code() + ": " + body);
            }
            JSONObject payload = new JSONObject(body);
            if (payload.has("error") && !payload.isNull("error")) {
                throw new IllegalStateException(method + " error: " + payload.getJSONObject("error"));
            }
            return payload;
        }
    }

    private long nextRequestId() {
        if (requestId % 7L == 0L) {
            requestId++;
        }
        return requestId++;
    }

    private long nextTimestampSec() {
        long timestamp = Instant.now().getEpochSecond();
        if (timestamp <= lastTimestampSec) {
            timestamp = lastTimestampSec + 1L;
        }
        long lastDigit = timestamp % 10L;
        if (lastDigit == 0L || lastDigit == 3L || lastDigit == 7L) {
            timestamp++;
        }
        lastTimestampSec = timestamp;
        return timestamp;
    }

    private Signature sign(String method, long id, String sid) throws Exception {
        String text = method + "~" + sysId + "~" + id + "~" + sid;
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        byte[] bytes = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder();
        for (byte value : bytes) {
            hex.append(String.format(Locale.ROOT, "%02x", value));
        }
        String hash = hex.toString();
        String guid = hash.substring(0, 8) + "-"
                + hash.substring(8, 12) + "-"
                + hash.substring(12, 16) + "-"
                + hash.substring(24, 28) + "-"
                + hash.substring(28, 40);
        return new Signature(hash.substring(16, 24), guid);
    }

    private static Instant parseSourceTime(String sourceTime, Instant observedAt) {
        try {
            LocalTime localTime = LocalTime.parse(sourceTime);
            LocalDate observedDate = LocalDateTime.ofInstant(observedAt, CITY_ZONE).toLocalDate();
            Instant candidate = LocalDateTime.of(observedDate, localTime).atZone(CITY_ZONE).toInstant();
            long diff = candidate.getEpochSecond() - observedAt.getEpochSecond();
            if (diff > 12L * 3600L) {
                candidate = candidate.minus(Duration.ofDays(1));
            } else if (diff < -12L * 3600L) {
                candidate = candidate.plus(Duration.ofDays(1));
            }
            return candidate;
        } catch (Exception ignored) {
            return observedAt;
        }
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    private static int parseInt(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception ignored) {
            return 0;
        }
    }

    private static double parseDouble(String value) {
        return Double.parseDouble(value.trim());
    }

    private static final class Signature {
        private final String magic;
        private final String guid;

        private Signature(String magic, String guid) {
            this.magic = magic;
            this.guid = guid;
        }
    }

    private static final class PollStats {
        private int units;
        private int written;
        private int unknownRoutes;
        private int stale;
        private int errors;
    }

    static final class NaviRouteMap {
        private final Map<String, Route> byNaviMrId;

        private NaviRouteMap(Map<String, Route> byNaviMrId) {
            this.byNaviMrId = byNaviMrId;
        }

        static NaviRouteMap load(String filePath) throws Exception {
            String content;
            if (filePath == null || filePath.isBlank()) {
                try (InputStream inputStream = BusNaviRealtimeClient.class.getClassLoader()
                        .getResourceAsStream("navi-route-map.json")) {
                    if (inputStream == null) {
                        throw new IllegalStateException("navi-route-map.json not found in classpath");
                    }
                    content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                content = Files.readString(Path.of(filePath), StandardCharsets.UTF_8);
            }

            JSONObject root = new JSONObject(content);
            JSONArray routes = root.getJSONArray("routes");
            Map<String, Route> result = new LinkedHashMap<>();
            for (int i = 0; i < routes.length(); i++) {
                JSONObject route = routes.getJSONObject(i);
                Route item = new Route(
                        route.getString("naviMrId"),
                        route.getString("naviTransportTypeId"),
                        route.getString("internalRouteId"),
                        route.getString("displayRouteNumber")
                );
                result.put(item.naviMrId, item);
            }
            return new NaviRouteMap(result);
        }

        Route byNaviMrId(String naviMrId) {
            return byNaviMrId.get(naviMrId);
        }

        int size() {
            return byNaviMrId.size();
        }

        static final class Route {
            private final String naviMrId;
            private final String naviTransportTypeId;
            private final String internalRouteId;
            private final String displayRouteNumber;

            private Route(
                    String naviMrId,
                    String naviTransportTypeId,
                    String internalRouteId,
                    String displayRouteNumber
            ) {
                this.naviMrId = naviMrId;
                this.naviTransportTypeId = naviTransportTypeId;
                this.internalRouteId = internalRouteId;
                this.displayRouteNumber = displayRouteNumber;
            }
        }
    }
}
