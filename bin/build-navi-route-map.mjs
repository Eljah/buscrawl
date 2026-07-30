#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs";

const baseUrl = process.env.NAVI_BASE_URL || "https://navi.kazantransport.ru/api";
const routesJsonPath = process.env.ROUTES_JSON_PATH || "src/main/resources/routes.json";
const outputPath = process.env.NAVI_ROUTE_MAP_OUTPUT || "src/main/resources/navi-route-map.json";

const oldRoutes = JSON.parse(fs.readFileSync(routesJsonPath, "utf8")).bus;
const oldByRouteKey = new Map();

for (const [internalRouteId, entry] of Object.entries(oldRoutes)) {
  const baseRouteNumber = String(entry[1] ?? "").toLowerCase();
  const transportType = Number(entry[5] ?? 0);
  const kind = transportType === 1 ? "trolleybus" : transportType === 2 ? "tram" : "bus";
  oldByRouteKey.set(`${kind}:${baseRouteNumber}`, internalRouteId);
}

let rid = 1;
let lastTimestamp = 0;

function nextRid() {
  if (rid % 7 === 0) {
    rid++;
  }
  return rid++;
}

function nextTimestamp() {
  let timestamp = Math.floor(Date.now() / 1000);
  if (timestamp <= lastTimestamp) {
    timestamp = lastTimestamp + 1;
  }
  const lastDigit = timestamp % 10;
  if (lastDigit === 0 || lastDigit === 3 || lastDigit === 7) {
    timestamp++;
  }
  lastTimestamp = timestamp;
  return timestamp;
}

function sign(method, id, sid) {
  const digest = crypto
    .createHash("sha1")
    .update(`${method}~kazan~${id}~${sid}`)
    .digest("hex");
  return {
    magic: digest.substring(16, 24),
    m: `${digest.substring(0, 8)}-${digest.substring(8, 12)}-${digest.substring(12, 16)}-${digest.substring(24, 28)}-${digest.substring(28, 40)}`,
  };
}

async function rpc(method, params = {}, signed = true) {
  const id = nextRid();
  const body = {
    jsonrpc: "2\u20242",
    method,
    ts: nextTimestamp(),
    params: { ...params },
    id,
  };
  let url = `${baseUrl}/rpc.php`;
  if (signed) {
    const signature = sign(method, id, params.sid || "");
    body.params.magic = signature.magic;
    url += `?m=${signature.m}`;
  }

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "origin": "https://navi.kazantransport.ru",
      "referer": "https://navi.kazantransport.ru/index.html",
      "user-agent": "Mozilla/5.0",
    },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} failed with HTTP ${response.status}: ${text.slice(0, 300)}`);
  }
  const payload = JSON.parse(text);
  if (payload.error) {
    throw new Error(`${method} failed: ${JSON.stringify(payload.error)}`);
  }
  return payload.result;
}

function naviTransportKind(ttId) {
  if (String(ttId) === "2") {
    return "trolleybus";
  }
  if (String(ttId) === "3") {
    return "tram";
  }
  return "bus";
}

function displayRouteNumber(ttId, routeNumber) {
  const value = String(routeNumber);
  return String(ttId) === "2" || String(ttId) === "3" ? `Т${value}` : value;
}

function directionCode(raceType, index) {
  const text = String(raceType || "").toUpperCase();
  if (text === "B" || text === "D") {
    return 1;
  }
  return index % 2;
}

async function main() {
  const session = await rpc("startSession", {}, false);
  const sid = session.sid;
  const tree = await rpc("getTransTypeTree", { sid, ok_id: "" });
  const routes = [];

  for (const transportType of tree) {
    const kind = naviTransportKind(transportType.tt_id);
    for (const routeSummary of transportType.routes || []) {
      const oldKey = `${kind}:${String(routeSummary.mr_num).toLowerCase()}`;
      const oldInternalRouteId = oldByRouteKey.get(oldKey);
      const internalRouteId = oldInternalRouteId || `navi-${routeSummary.mr_id}`;
      const route = await rpc("getRoute", { sid, mr_id: routeSummary.mr_id });
      const races = Object.values(route.races || {}).map((race, index) => ({
        naviRaceId: String(race.rl_id || ""),
        naviRaceType: String(race.rl_racetype || ""),
        direction: directionCode(race.rl_racetype, index),
        firstStopName: race.stopList?.[0]?.st_title || "",
        lastStopName: race.stopList?.[race.stopList.length - 1]?.st_title || "",
        stops: (race.stopList || []).map((stop, order) => ({
          naviStopId: String(stop.st_id),
          stopId: `navi-${stop.st_id}`,
          name: String(stop.st_title || stop.st_title_en || stop.st_id),
          lat: Number(stop.st_lat),
          lon: Number(stop.st_long),
          order,
        })),
      }));

      routes.push({
        naviMrId: String(routeSummary.mr_id),
        naviRouteNumber: String(routeSummary.mr_num),
        naviRouteTitle: String(routeSummary.mr_title || ""),
        naviTransportTypeId: String(transportType.tt_id),
        naviTransportTypeName: String(transportType.tt_title || ""),
        internalRouteId,
        displayRouteNumber: displayRouteNumber(transportType.tt_id, routeSummary.mr_num),
        mappedFrom: oldInternalRouteId ? "routes.json" : "navi-overlay",
        races,
      });
    }
  }

  routes.sort((left, right) => {
    const byType = Number(left.naviTransportTypeId) - Number(right.naviTransportTypeId);
    if (byType !== 0) {
      return byType;
    }
    return left.displayRouteNumber.localeCompare(right.displayRouteNumber, "ru", { numeric: true });
  });

  const payload = {
    generatedAt: new Date().toISOString(),
    source: "https://navi.kazantransport.ru/index.html",
    apiBaseUrl: baseUrl,
    city: "kazan",
    mappingPolicy: "Map by transport type and route number to existing routes.json ids; use stable navi-<mr_id> ids with route topology overlay when no existing id exists.",
    routes,
  };

  fs.mkdirSync(outputPath.replace(/[\\/][^\\/]+$/, ""), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  const bySource = routes.reduce((acc, route) => {
    acc[route.mappedFrom] = (acc[route.mappedFrom] || 0) + 1;
    return acc;
  }, {});
  console.log(`Wrote ${outputPath}`);
  console.log(`Routes: ${routes.length}; ${JSON.stringify(bySource)}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
