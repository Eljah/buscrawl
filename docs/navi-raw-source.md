# Navi Raw Source

`https://navi.kazantransport.ru/index.html` is a polling JSON-RPC web application. It does not use WebSocket for live vehicle coordinates.

The site loads `conf/conf.json`, where:

- `baseUrl` points to `https://navi.kazantransport.ru/api`.
- `unitUpdateInt` is the vehicle polling interval in seconds.

The raw client uses:

1. `POST /api/rpc.php`, method `startSession`, without request signing.
2. `POST /api/rpc.php?m=<signature>`, method `getUnitsInRect`, with a city bounding box.

Signed requests reproduce the browser code:

```text
sha1(method + "~kazan~" + id + "~" + sid)
```

The URL query `m` is assembled from the hash as a UUID-like string. `params.magic` is `hash[16:24]`.

The vehicle payload contains `u_id`, `tt_id`, `mr_id`, `mr_num`, `u_statenum`, `u_garagnum`, `u_timenav`, `u_lat`, `u_long`, `u_speed`, `u_course`, and route direction fields.

`BusNaviRealtimeClient` writes the same JSONL spool schema as `BusRealtimeClient`, so `BusDataSparkStreaming` and downstream parquet jobs do not need a schema change. `timestamp` is the ingestion time in UTC seconds; `sourceTimestamp` is parsed from Navi local Moscow `u_timenav`.

Route ids are configured in `src/main/resources/navi-route-map.json`. Existing routes are mapped by transport type and route number to current `routes.json` ids. Navi-only routes get stable ids `navi-<mr_id>` and are exposed to `RouteTopology` through the same config as an overlay.
