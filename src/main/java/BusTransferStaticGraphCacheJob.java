import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class BusTransferStaticGraphCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String routesPath = System.getenv().getOrDefault("BUS_ROUTES_JSON", "");
        Path outputDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_GRAPH_DIR",
                "./var/bus/transfer-static-graph"
        ));
        int maxTransfers = Integer.parseInt(System.getenv().getOrDefault("BUS_TRANSFER_STATIC_MAX_TRANSFERS", "3"));
        double walkRadiusMeters = Double.parseDouble(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_WALK_RADIUS_METERS",
                "300"
        ));
        int maxSettledStatesPerOrigin = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_MAX_SETTLED_STATES_PER_ORIGIN",
                "250000"
        ));

        Files.createDirectories(outputDir);
        RouteTopology topology = RouteTopology.load(routesPath);
        Graph graph = Graph.build(topology, walkRadiusMeters);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("createdAt", Instant.now().toString());
        metadata.put("maxTransfers", maxTransfers);
        metadata.put("walkRadiusMeters", walkRadiusMeters);
        metadata.put("stops", graph.stopsById.size());
        metadata.put("rideEdges", graph.rideEdgesByStart.values().stream().mapToInt(List::size).sum());
        metadata.put("walkEdges", graph.walkEdgesByStart.values().stream().mapToInt(List::size).sum());
        Files.writeString(
                outputDir.resolve("metadata.json"),
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata) + System.lineSeparator(),
                StandardCharsets.UTF_8
        );

        for (int transferLimit = 1; transferLimit <= maxTransfers; transferLimit++) {
            Path outputFile = outputDir.resolve("paths-max-transfers-" + transferLimit + ".jsonl");
            long rows = 0L;
            try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
                for (RouteTopology.StopInfo origin : graph.sortedStops) {
                    Map<String, Label> bestByDestination = shortestPaths(
                            graph,
                            origin.getStopId(),
                            transferLimit + 1,
                            maxSettledStatesPerOrigin
                    );
                    for (RouteTopology.StopInfo destination : graph.sortedStops) {
                        if (origin.getStopId().equals(destination.getStopId())) {
                            continue;
                        }
                        Label label = bestByDestination.get(destination.getStopId());
                        if (label == null || label.rides == 0) {
                            continue;
                        }
                        writer.write(MAPPER.writeValueAsString(toRecord(origin, destination, label, transferLimit)));
                        writer.newLine();
                        rows++;
                    }
                }
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferStaticGraphCacheJob: wrote %,d rows to %s%n",
                    rows,
                    outputFile.toAbsolutePath()
            );
        }
    }

    private static Map<String, Label> shortestPaths(
            Graph graph,
            String originStopId,
            int maxRides,
            int maxSettledStates
    ) {
        PriorityQueue<Label> queue = new PriorityQueue<>(Comparator.comparingDouble(label -> label.distanceMeters));
        Map<StateKey, Double> bestStateDistance = new HashMap<>();
        Map<String, Label> bestByDestination = new HashMap<>();
        Label start = new Label(originStopId, 0, 0.0, null, null);
        queue.add(start);
        bestStateDistance.put(new StateKey(originStopId, 0), 0.0);
        int settled = 0;

        while (!queue.isEmpty() && settled < maxSettledStates) {
            Label current = queue.poll();
            StateKey currentKey = new StateKey(current.stopId, current.rides);
            double knownDistance = bestStateDistance.getOrDefault(currentKey, Double.POSITIVE_INFINITY);
            if (current.distanceMeters > knownDistance + 0.001) {
                continue;
            }
            settled++;
            if (current.rides > 0) {
                bestByDestination.merge(
                        current.stopId,
                        current,
                        (oldLabel, newLabel) -> oldLabel.distanceMeters <= newLabel.distanceMeters ? oldLabel : newLabel
                );
            }

            if (current.rides < maxRides) {
                for (RideEdge edge : graph.rideEdgesByStart.getOrDefault(current.stopId, List.of())) {
                    addLabel(queue, bestStateDistance, new Label(
                            edge.endStopId,
                            current.rides + 1,
                            current.distanceMeters + edge.distanceMeters,
                            current,
                            edge
                    ));
                }
            }
            for (WalkEdge edge : graph.walkEdgesByStart.getOrDefault(current.stopId, List.of())) {
                addLabel(queue, bestStateDistance, new Label(
                        edge.endStopId,
                        current.rides,
                        current.distanceMeters + edge.distanceMeters,
                        current,
                        edge
                ));
            }
        }
        return bestByDestination;
    }

    private static void addLabel(
            PriorityQueue<Label> queue,
            Map<StateKey, Double> bestStateDistance,
            Label next
    ) {
        StateKey key = new StateKey(next.stopId, next.rides);
        double oldDistance = bestStateDistance.getOrDefault(key, Double.POSITIVE_INFINITY);
        if (next.distanceMeters + 0.001 >= oldDistance) {
            return;
        }
        bestStateDistance.put(key, next.distanceMeters);
        queue.add(next);
    }

    private static Map<String, Object> toRecord(
            RouteTopology.StopInfo origin,
            RouteTopology.StopInfo destination,
            Label label,
            int transferLimit
    ) {
        List<PathEdge> edges = new ArrayList<>();
        for (Label cursor = label; cursor != null && cursor.edge != null; cursor = cursor.previous) {
            edges.add(0, cursor.edge);
        }
        List<Map<String, Object>> legs = edges.stream()
                .filter(edge -> edge instanceof RideEdge)
                .map(edge -> ((RideEdge) edge).toMap())
                .collect(Collectors.toList());
        List<Map<String, Object>> walks = edges.stream()
                .filter(edge -> edge instanceof WalkEdge)
                .map(edge -> ((WalkEdge) edge).toMap())
                .collect(Collectors.toList());

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("originStopId", origin.getStopId());
        record.put("originStopName", origin.getStopName());
        record.put("destinationStopId", destination.getStopId());
        record.put("destinationStopName", destination.getStopName());
        record.put("maxTransfers", transferLimit);
        record.put("transferCount", Math.max(0, label.rides - 1));
        record.put("rideCount", label.rides);
        record.put("totalDistanceMeters", Math.round(label.distanceMeters));
        record.put("routeDistanceMeters", Math.round(legs.stream()
                .mapToDouble(leg -> ((Number) leg.get("distanceMeters")).doubleValue())
                .sum()));
        record.put("walkDistanceMeters", Math.round(walks.stream()
                .mapToDouble(walk -> ((Number) walk.get("distanceMeters")).doubleValue())
                .sum()));
        record.put("edges", edges.stream()
                .map(edge -> edge instanceof RideEdge ? ((RideEdge) edge).toMap() : ((WalkEdge) edge).toMap())
                .collect(Collectors.toList()));
        record.put("legs", legs);
        record.put("walks", walks);
        return record;
    }

    private interface PathEdge {
    }

    private static final class Graph {
        private final Map<String, RouteTopology.StopInfo> stopsById;
        private final List<RouteTopology.StopInfo> sortedStops;
        private final Map<String, List<RideEdge>> rideEdgesByStart;
        private final Map<String, List<WalkEdge>> walkEdgesByStart;

        private Graph(
                Map<String, RouteTopology.StopInfo> stopsById,
                List<RouteTopology.StopInfo> sortedStops,
                Map<String, List<RideEdge>> rideEdgesByStart,
                Map<String, List<WalkEdge>> walkEdgesByStart
        ) {
            this.stopsById = stopsById;
            this.sortedStops = sortedStops;
            this.rideEdgesByStart = rideEdgesByStart;
            this.walkEdgesByStart = walkEdgesByStart;
        }

        private static Graph build(RouteTopology topology, double walkRadiusMeters) {
            Map<String, RouteTopology.StopInfo> stopsById = topology.getStops().stream()
                    .collect(Collectors.toMap(
                            RouteTopology.StopInfo::getStopId,
                            stop -> stop,
                            (left, right) -> left,
                            LinkedHashMap::new
                    ));
            List<RouteTopology.StopInfo> sortedStops = new ArrayList<>(stopsById.values());
            sortedStops.sort(Comparator.comparing(RouteTopology.StopInfo::getStopId));

            Map<String, List<RouteTopology.RouteStopInfo>> byRouteDirection = topology.getRouteStops().stream()
                    .collect(Collectors.groupingBy(
                            routeStop -> routeStop.getInternalRouteId() + "|" + routeStop.getDirection(),
                            LinkedHashMap::new,
                            Collectors.toList()
                    ));
            Map<String, List<RideEdge>> rideEdgesByStart = new HashMap<>();
            for (Collection<RouteTopology.RouteStopInfo> rawStops : byRouteDirection.values()) {
                List<RouteTopology.RouteStopInfo> routeStops = new ArrayList<>(rawStops);
                routeStops.sort(Comparator.comparingInt(RouteTopology.RouteStopInfo::getStopOrder));
                double[] cumulative = new double[routeStops.size()];
                for (int i = 1; i < routeStops.size(); i++) {
                    RouteTopology.RouteStopInfo previous = routeStops.get(i - 1);
                    RouteTopology.RouteStopInfo current = routeStops.get(i);
                    cumulative[i] = cumulative[i - 1] + haversineMeters(
                            previous.getStopLatitude(),
                            previous.getStopLongitude(),
                            current.getStopLatitude(),
                            current.getStopLongitude()
                    );
                }
                for (int i = 0; i < routeStops.size(); i++) {
                    RouteTopology.RouteStopInfo start = routeStops.get(i);
                    for (int j = i + 1; j < routeStops.size(); j++) {
                        RouteTopology.RouteStopInfo end = routeStops.get(j);
                        RideEdge edge = new RideEdge(
                                start.getStopId(),
                                end.getStopId(),
                                start.getStopName(),
                                end.getStopName(),
                                start.getInternalRouteId(),
                                start.getRouteNumber(),
                                start.getDirection(),
                                start.getStopOrder(),
                                end.getStopOrder(),
                                cumulative[j] - cumulative[i]
                        );
                        rideEdgesByStart.computeIfAbsent(start.getStopId(), ignored -> new ArrayList<>()).add(edge);
                    }
                }
            }

            Map<String, List<WalkEdge>> walkEdgesByStart = new HashMap<>();
            for (int i = 0; i < sortedStops.size(); i++) {
                RouteTopology.StopInfo left = sortedStops.get(i);
                for (int j = i + 1; j < sortedStops.size(); j++) {
                    RouteTopology.StopInfo right = sortedStops.get(j);
                    double distance = haversineMeters(
                            left.getLatitude(),
                            left.getLongitude(),
                            right.getLatitude(),
                            right.getLongitude()
                    );
                    if (distance > walkRadiusMeters) {
                        continue;
                    }
                    walkEdgesByStart.computeIfAbsent(left.getStopId(), ignored -> new ArrayList<>())
                            .add(new WalkEdge(left.getStopId(), right.getStopId(), left.getStopName(), right.getStopName(), distance));
                    walkEdgesByStart.computeIfAbsent(right.getStopId(), ignored -> new ArrayList<>())
                            .add(new WalkEdge(right.getStopId(), left.getStopId(), right.getStopName(), left.getStopName(), distance));
                }
            }
            return new Graph(stopsById, sortedStops, rideEdgesByStart, walkEdgesByStart);
        }
    }

    private static final class RideEdge implements PathEdge {
        private final String startStopId;
        private final String endStopId;
        private final String startStopName;
        private final String endStopName;
        private final String internalRouteId;
        private final String routeNumber;
        private final int direction;
        private final int startStopOrder;
        private final int endStopOrder;
        private final double distanceMeters;

        private RideEdge(
                String startStopId,
                String endStopId,
                String startStopName,
                String endStopName,
                String internalRouteId,
                String routeNumber,
                int direction,
                int startStopOrder,
                int endStopOrder,
                double distanceMeters
        ) {
            this.startStopId = startStopId;
            this.endStopId = endStopId;
            this.startStopName = startStopName;
            this.endStopName = endStopName;
            this.internalRouteId = internalRouteId;
            this.routeNumber = routeNumber;
            this.direction = direction;
            this.startStopOrder = startStopOrder;
            this.endStopOrder = endStopOrder;
            this.distanceMeters = distanceMeters;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("type", "ride");
            item.put("internalRouteId", internalRouteId);
            item.put("routeNumber", routeNumber);
            item.put("direction", direction);
            item.put("fromStopId", startStopId);
            item.put("fromStopName", startStopName);
            item.put("toStopId", endStopId);
            item.put("toStopName", endStopName);
            item.put("fromStopOrder", startStopOrder);
            item.put("toStopOrder", endStopOrder);
            item.put("distanceMeters", distanceMeters);
            return item;
        }
    }

    private static final class WalkEdge implements PathEdge {
        private final String startStopId;
        private final String endStopId;
        private final String startStopName;
        private final String endStopName;
        private final double distanceMeters;

        private WalkEdge(
                String startStopId,
                String endStopId,
                String startStopName,
                String endStopName,
                double distanceMeters
        ) {
            this.startStopId = startStopId;
            this.endStopId = endStopId;
            this.startStopName = startStopName;
            this.endStopName = endStopName;
            this.distanceMeters = distanceMeters;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("type", "walk");
            item.put("fromStopId", startStopId);
            item.put("fromStopName", startStopName);
            item.put("toStopId", endStopId);
            item.put("toStopName", endStopName);
            item.put("distanceMeters", distanceMeters);
            return item;
        }
    }

    private static final class Label {
        private final String stopId;
        private final int rides;
        private final double distanceMeters;
        private final Label previous;
        private final PathEdge edge;

        private Label(String stopId, int rides, double distanceMeters, Label previous, PathEdge edge) {
            this.stopId = stopId;
            this.rides = rides;
            this.distanceMeters = distanceMeters;
            this.previous = previous;
            this.edge = edge;
        }
    }

    private static final class StateKey {
        private final String stopId;
        private final int rides;

        private StateKey(String stopId, int rides) {
            this.stopId = stopId;
            this.rides = rides;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof StateKey)) {
                return false;
            }
            StateKey that = (StateKey) other;
            return rides == that.rides && stopId.equals(that.stopId);
        }

        @Override
        public int hashCode() {
            return 31 * stopId.hashCode() + rides;
        }
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double earthRadiusMeters = 6_371_000;
        double phi1 = Math.toRadians(lat1);
        double phi2 = Math.toRadians(lat2);
        double dPhi = Math.toRadians(lat2 - lat1);
        double dLambda = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
                + Math.cos(phi1) * Math.cos(phi2)
                * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
        return 2 * earthRadiusMeters * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
