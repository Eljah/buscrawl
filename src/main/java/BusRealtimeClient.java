import io.socket.client.IO;
import io.socket.client.Socket;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.logging.HttpLoggingInterceptor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BusRealtimeClient {
    private static final ZoneId CITY_ZONE = ZoneId.of(
            System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow")
    );
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd HH:mm:ss",
            Locale.getDefault()
    );
    private static final boolean VERBOSE_BUS_LOG =
            Boolean.parseBoolean(System.getenv().getOrDefault("BUS_VERBOSE_LOG", "false"));
    private static final boolean ROUTE_UPDATE_LOG =
            Boolean.parseBoolean(System.getenv().getOrDefault("BUS_ROUTE_UPDATE_LOG", "false"));
    private static final boolean TIMESTAMP_DEBUG_LOG =
            Boolean.parseBoolean(System.getenv().getOrDefault("BUS_TIMESTAMP_DEBUG_LOG", "false"));
    private static final boolean MODE10_DEBUG_LOG =
            Boolean.parseBoolean(System.getenv().getOrDefault("BUS_MODE10_DEBUG_LOG", "false"));
    private static final boolean MODE11_DEBUG_LOG =
            Boolean.parseBoolean(System.getenv().getOrDefault("BUS_MODE11_DEBUG_LOG", "false"));
    private static final Set<String> SUBSCRIBED_ROUTE_IDS = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) throws Exception {
        String url = "https://ru.busti.me";

        RouteMapper routeMapper = new RouteMapper(System.getenv("ROUTES_JSON_PATH"));
        int tcpPort = Integer.parseInt(System.getenv().getOrDefault("BUS_TCP_PORT", "9999"));
        BusDataTcpServer tcpServer = new BusDataTcpServer(tcpPort);

        new Thread(() -> {
            try {
                tcpServer.start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "bus-data-tcp-server").start();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request().newBuilder()
                            .header("Origin", "https://ru.busti.me")
                            .header("Referer", "https://ru.busti.me/kazan/")
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/136.0.0.0 Safari/537.36")
                            .build();
                    return chain.proceed(request);
                })
                .addInterceptor(new HttpLoggingInterceptor(System.out::println)
                        .setLevel(HttpLoggingInterceptor.Level.BASIC))
                .build();

        IO.setDefaultOkHttpCallFactory(client);
        IO.setDefaultOkHttpWebSocketFactory(client);

        IO.Options options = new IO.Options();
        options.transports = new String[]{"polling", "websocket"};

        Socket socket = IO.socket(url, options);

        socket.on(Socket.EVENT_CONNECT, args1 -> {
            System.out.println("Connected successfully");
            SUBSCRIBED_ROUTE_IDS.clear();

            send(socket, "2probe");
            send(socket, "3probe");
            send(socket, "5");

            emit(socket, "join", "ru.bustime.bus_mode10__960");
            emit(socket, "join", "ru.bustime.bus_mode11__960");
            emit(socket, "join", "ru.bustime.counters");
            emit(socket, "join", "ru.bustime.bus_amounts__10");
            emit(socket, "join", "ru.bustime.us__1650490321");
            emit(socket, "join", "ru.bustime.city__10");
            emit(socket, "join", "ru.bustime.taxi__10");
            emit(socket, "join", "ru.bustime.reload_soft__10");

            JSONObject rpcMode10 = new JSONObject()
                    .put("bus_id", "960")
                    .put("mode", 10)
                    .put("mobile", 0);
            emit(socket, "rpc_bdata", rpcMode10);

            JSONObject rpcMode11 = new JSONObject()
                    .put("bus_id", "960")
                    .put("mode", 11)
                    .put("mobile", 0);
            emit(socket, "rpc_bdata", rpcMode11);

            if (MODE11_DEBUG_LOG) {
                socket.on("ru.bustime.bus_mode11__960", mode11Args -> {
                    if (mode11Args.length == 0 || !(mode11Args[0] instanceof JSONObject)) {
                        System.out.println("Mode11 debug: unexpected payload");
                        return;
                    }
                    JSONObject payload = (JSONObject) mode11Args[0];
                    System.out.println("Mode11 debug payload keys: " + payload.keySet());
                    System.out.println("Mode11 debug payload: " + payload);
                });
            }
        });

        socket.on("ru.bustime.bus_amounts__10", args1 -> {
            JSONObject data = (JSONObject) args1[0];
            JSONObject amounts = data.getJSONObject("busamounts");

            System.out.println("Bus amounts received: " + amounts);

            for (String key : amounts.keySet()) {
                String internalRouteId = key.split("_")[0];
                if (SUBSCRIBED_ROUTE_IDS.add(internalRouteId)) {
                    subscribeRoute(socket, routeMapper, tcpServer, internalRouteId);
                }
            }
        });

        socket.connect();
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void subscribeRoute(Socket socket, RouteMapper routeMapper,
                                       BusDataTcpServer tcpServer, String internalRouteId) {
        String eventName = "ru.bustime.bus_mode10__" + internalRouteId;

        socket.on(eventName, args -> {
            JSONObject data = (JSONObject) args[0];
            if (MODE10_DEBUG_LOG) {
                System.out.println("Mode10 debug payload keys: " + data.keySet());
                System.out.println("Mode10 debug payload: " + data);
            }
            if (data.has("bdata_mode10")) {
                handleBusData(routeMapper, tcpServer, internalRouteId, data.getJSONObject("bdata_mode10"));
            }
        });

        emit(socket, "join", eventName);
    }

    private static void handleBusData(RouteMapper routeMapper, BusDataTcpServer tcpServer,
                                      String internalRouteId, JSONObject busData) {
        JSONArray buses = busData.getJSONArray("l");
        String realRouteNumber = routeMapper.getRealRouteNumber(internalRouteId);
        long minTimestampSec = Long.MAX_VALUE;
        long maxTimestampSec = Long.MIN_VALUE;
        if (ROUTE_UPDATE_LOG) {
            System.out.printf("Route update: %s (ID: %s), buses: %d%n", realRouteNumber, internalRouteId, buses.length());
        }

        for (int i = 0; i < buses.length(); i++) {
            JSONObject bus = buses.getJSONObject(i);
            double latitude = bus.getDouble("y");
            double longitude = bus.getDouble("x");
            int speed = bus.getInt("s");
            String plate = bus.getString("g");
            long sourceTimestampSec = bus.getLong("ts");
            minTimestampSec = Math.min(minTimestampSec, sourceTimestampSec);
            maxTimestampSec = Math.max(maxTimestampSec, sourceTimestampSec);

            Instant observedAt = Instant.now();
            long timestampSec = observedAt.getEpochSecond();
            String readableTime = DATE_FORMAT.format(observedAt.atZone(CITY_ZONE));
            String sourceReadableTime = DATE_FORMAT.format(Instant.ofEpochSecond(sourceTimestampSec).atZone(CITY_ZONE));

            JSONObject jsonBusData = new JSONObject()
                    .put("internalRouteId", internalRouteId)
                    .put("realRouteNumber", realRouteNumber)
                    .put("latitude", latitude)
                    .put("longitude", longitude)
                    .put("speed", speed)
                    .put("plate", plate)
                    .put("timestamp", timestampSec)
                    .put("readableTime", readableTime)
                    .put("sourceTimestamp", sourceTimestampSec)
                    .put("sourceReadableTime", sourceReadableTime);

            tcpServer.sendData(jsonBusData.toString());

            if (VERBOSE_BUS_LOG) {
                System.out.printf(
                        "Bus route: %s (ID: %s), bus: %s%n" +
                                "    Coordinates: [%.6f, %.6f]%n" +
                                "    Updated at: %s%n" +
                                "    Speed: %d km/h%n%n",
                        realRouteNumber, internalRouteId, plate,
                        latitude, longitude, readableTime, speed
                );
            }
        }

        if (TIMESTAMP_DEBUG_LOG && maxTimestampSec > 0L) {
            long nowSec = Instant.now().getEpochSecond();
            System.out.printf(
                    "Timestamp debug: route=%s id=%s buses=%d minTs=%d maxTs=%d maxLagSec=%d%n",
                    realRouteNumber,
                    internalRouteId,
                    buses.length(),
                    minTimestampSec,
                    maxTimestampSec,
                    nowSec - maxTimestampSec
            );
        }
    }

    private static void emit(Socket socket, String event, Object payload) {
        System.out.printf("Emit: %s, Payload: %s%n", event, payload);
        socket.emit(event, payload);
    }

    private static void send(Socket socket, String message) {
        System.out.printf("Send: %s%n", message);
        socket.send(message);
    }
}
