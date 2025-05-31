import io.socket.client.IO;
import io.socket.client.Socket;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.logging.HttpLoggingInterceptor;
import org.json.JSONArray;
import org.json.JSONObject;

public class BusRealtimeClient {
    public static void main(String[] args) throws Exception {
        String url = "https://ru.busti.me";

        // Настраиваем HTTP-клиент с логированием
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request().newBuilder()
                            .header("Origin", "https://ru.busti.me")
                            .header("Referer", "https://ru.busti.me/kazan/")
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
                                    "AppleWebKit/537.36 (KHTML, like Gecko) " +
                                    "Chrome/136.0.0.0 Safari/537.36")
                            .build();
                    return chain.proceed(request);
                })
                .addInterceptor(new HttpLoggingInterceptor(System.out::println)
                        .setLevel(HttpLoggingInterceptor.Level.BODY))
                .build();

        IO.setDefaultOkHttpCallFactory(client);
        IO.setDefaultOkHttpWebSocketFactory(client);

        IO.Options options = new IO.Options();
        options.transports = new String[]{"polling", "websocket"};

        Socket socket = IO.socket(url, options);

        socket.on(Socket.EVENT_CONNECT, args1 -> {
            System.out.println("✅ Connected successfully");

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

            JSONObject rpc_mode10 = new JSONObject()
                    .put("bus_id", "960")
                    .put("mode", 10)
                    .put("mobile", 0);
            emit(socket, "rpc_bdata", rpc_mode10);

            JSONObject rpc_mode11 = new JSONObject()
                    .put("bus_id", "960")
                    .put("mode", 11)
                    .put("mobile", 0);
            emit(socket, "rpc_bdata", rpc_mode11);
        });

        socket.on("ru.bustime.bus_mode10__960", args1 -> {
            JSONObject data = (JSONObject) args1[0];
            if (data.has("bdata_mode10")) {
                handleBusData(data.getJSONObject("bdata_mode10"));
            } else if (data.has("busamounts")) {
                handleBusAmounts(data.getJSONObject("busamounts"));
            } else {
                System.out.println("⚠️ Unhandled message: " + data);
            }
        });

        // Обработка стандартных событий отдельно (без onAny)
        logEvent(socket, Socket.EVENT_CONNECT);
        logEvent(socket, Socket.EVENT_DISCONNECT);
        logEvent(socket, Socket.EVENT_CONNECT_ERROR);
        logEvent(socket, Socket.EVENT_ERROR);
        logEvent(socket, Socket.EVENT_MESSAGE);
        logEvent(socket, Socket.EVENT_PING);
        logEvent(socket, Socket.EVENT_PONG);

        socket.connect();
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void handleBusData(JSONObject busData) {
        JSONArray buses = busData.getJSONArray("l");
        for (int i = 0; i < buses.length(); i++) {
            JSONObject bus = buses.getJSONObject(i);
            double latitude = bus.getDouble("y");
            double longitude = bus.getDouble("x");
            int speed = bus.getInt("s");
            String plate = bus.getString("g");

            System.out.printf("🚌 Bus %s at [%f, %f], speed: %d km/h%n",
                    plate, latitude, longitude, speed);
        }
    }

    private static void handleBusAmounts(JSONObject amounts) {
        System.out.println("📊 Bus amounts: " + amounts.toString());
    }

    private static void emit(Socket socket, String event, Object payload) {
        System.out.printf("📤 [CLIENT -> SERVER] Emit: %s, Payload: %s%n", event, payload);
        socket.emit(event, payload);
    }

    private static void send(Socket socket, String message) {
        System.out.printf("📤 [CLIENT -> SERVER] Send: %s%n", message);
        socket.send(message);
    }

    private static void logEvent(Socket socket, String event) {
        socket.on(event, args -> {
            System.out.printf("📥 [SERVER -> CLIENT] Event: %s, Args: ", event);
            for (Object arg : args) {
                System.out.print(arg + "; ");
            }
            System.out.println();
        });
    }
}
