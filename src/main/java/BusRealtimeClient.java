import io.socket.client.IO;
import io.socket.client.Socket;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.logging.HttpLoggingInterceptor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class BusRealtimeClient {
    private static final SimpleDateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());

    public static void main(String[] args) throws Exception {
        String url = "https://ru.busti.me";

        // загружаем данные маршрутов из JSON
        RouteMapper routeMapper = new RouteMapper();

        // настройка клиента с логированием запросов
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
            System.out.println("✅ Connected successfully");
            emit(socket, "join", "ru.bustime.bus_amounts__10");
        });

        socket.on("ru.bustime.bus_amounts__10", args1 -> {
            JSONObject data = (JSONObject) args1[0];
            JSONObject amounts = data.getJSONObject("busamounts");

            for (String key : amounts.keySet()) {
                String internalRouteId = key.split("_")[0];
                String realRouteNumber = routeMapper.getRouteNumberByInternalId(internalRouteId);

                System.out.printf("📌 Подписка на маршрут: %s (внутренний ID: %s)%n",
                        realRouteNumber, internalRouteId);

                subscribeRoute(socket, internalRouteId, realRouteNumber);
            }
        });

        socket.connect();
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void subscribeRoute(Socket socket, String internalRouteId, String realRouteNumber) {
        String eventName = "ru.bustime.bus_mode10__" + internalRouteId;

        socket.on(eventName, args -> {
            JSONObject data = (JSONObject) args[0];
            if (data.has("bdata_mode10")) {
                handleBusData(internalRouteId, realRouteNumber, data.getJSONObject("bdata_mode10"));
            }
        });

        emit(socket, "join", eventName);
    }

    private static void handleBusData(String internalRouteId, String realRouteNumber, JSONObject busData) {
        JSONArray buses = busData.getJSONArray("l");

        for (int i = 0; i < buses.length(); i++) {
            JSONObject bus = buses.getJSONObject(i);
            double latitude = bus.getDouble("y");
            double longitude = bus.getDouble("x");
            int speed = bus.getInt("s");
            String plate = bus.getString("g");
            long timestampSec = bus.getLong("ts");
            String readableTime = dateFormat.format(new Date(timestampSec * 1000));

            System.out.printf("🚌 Маршрут: %s (ID: %s), автобус: %s%n" +
                            "    📍 Координаты: [%.6f, %.6f]%n" +
                            "    ⏰ Время обновления: %s%n" +
                            "    🚀 Скорость: %d км/ч%n%n",
                    realRouteNumber, internalRouteId, plate,
                    latitude, longitude, readableTime, speed);
        }
    }

    private static void emit(Socket socket, String event, Object payload) {
        System.out.printf("📤 [CLIENT -> SERVER] Emit: %s, Payload: %s%n", event, payload);
        socket.emit(event, payload);
    }
}
