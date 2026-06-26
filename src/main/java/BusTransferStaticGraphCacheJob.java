import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class BusTransferStaticGraphCacheJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int BINARY_MAGIC = 0x42534731; // BSG1
    private static final int BINARY_VERSION = 1;
    private static final Comparator<Label> LABEL_ORDER = Comparator
            .comparingInt((Label label) -> label.rides)
            .thenComparingDouble(label -> label.distanceMeters)
            .thenComparing(Label::pathSignature);

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
        double stopClusterRadiusMeters = Double.parseDouble(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_STOP_CLUSTER_RADIUS_METERS",
                "180"
        ));
        int maxSettledStatesPerOrigin = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_MAX_SETTLED_STATES_PER_ORIGIN",
                "250000"
        ));
        int maxAlternativesPerState = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_MAX_ALTERNATIVES_PER_STATE",
                "6"
        ));
        int maxAlternativesPerDestination = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_MAX_ALTERNATIVES_PER_DESTINATION",
                "8"
        ));
        double alternativeDistanceRatio = Double.parseDouble(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_ALTERNATIVE_DISTANCE_RATIO",
                "1.5"
        ));
        boolean writeJsonl = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_TRANSFER_STATIC_WRITE_JSONL",
                "false"
        ));

        Files.createDirectories(outputDir);
        RouteTopology topology = RouteTopology.load(routesPath);
        Graph graph = Graph.build(topology, walkRadiusMeters, stopClusterRadiusMeters);
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("createdAt", Instant.now().toString());
        metadata.put("maxTransfers", maxTransfers);
        metadata.put("walkRadiusMeters", walkRadiusMeters);
        metadata.put("stopClusterRadiusMeters", stopClusterRadiusMeters);
        metadata.put("maxAlternativesPerState", maxAlternativesPerState);
        metadata.put("maxAlternativesPerDestination", maxAlternativesPerDestination);
        metadata.put("alternativeDistanceRatio", alternativeDistanceRatio);
        metadata.put("format", writeJsonl ? "binary+jsonl" : "binary");
        metadata.put("stops", graph.stopsById.size());
        metadata.put("rideEdges", graph.rideEdgesByStart.values().stream().mapToInt(List::size).sum());
        metadata.put("walkEdges", graph.walkEdgesByStart.values().stream().mapToInt(List::size).sum());
        Files.writeString(
                outputDir.resolve("metadata.json"),
                MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata) + System.lineSeparator(),
                StandardCharsets.UTF_8
        );

        for (int transferLimit = 1; transferLimit <= maxTransfers; transferLimit++) {
            Path binaryOutputDir = outputDir.resolve("paths-max-transfers-" + transferLimit + "-by-origin");
            Path jsonOutputFile = outputDir.resolve("paths-max-transfers-" + transferLimit + ".jsonl");
            Files.createDirectories(binaryOutputDir);
            long rows;
            try (BufferedWriter jsonWriter = writeJsonl ? Files.newBufferedWriter(jsonOutputFile, StandardCharsets.UTF_8) : null) {
                rows = 0L;
                for (RouteTopology.StopInfo origin : graph.sortedStops) {
                    Map<String, List<Label>> bestByDestination = shortestPathAlternatives(
                            graph,
                            origin.getStopId(),
                            transferLimit + 1,
                            maxSettledStatesPerOrigin,
                            maxAlternativesPerState,
                            maxAlternativesPerDestination,
                            alternativeDistanceRatio
                    );
                    List<BinaryCandidate> originCandidates = new ArrayList<>();
                    for (RouteTopology.StopInfo destination : graph.sortedStops) {
                        if (origin.getStopId().equals(destination.getStopId())) {
                            continue;
                        }
                        List<Label> labels = destinationAlternatives(
                                graph,
                                bestByDestination,
                                destination,
                                maxAlternativesPerDestination,
                                alternativeDistanceRatio
                        );
                        if (labels == null || labels.isEmpty()) {
                            continue;
                        }
                        for (int i = 0; i < labels.size(); i++) {
                            Label label = labels.get(i);
                            originCandidates.add(new BinaryCandidate(destination.getStopId(), label, i));
                            if (jsonWriter != null) {
                                jsonWriter.write(MAPPER.writeValueAsString(toRecord(origin, destination, label, transferLimit, i)));
                                jsonWriter.newLine();
                            }
                            rows++;
                        }
                    }
                    Path originFile = binaryOutputDir.resolve(origin.getStopId() + ".bin");
                    try (DataOutputStream binaryWriter = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(originFile)))) {
                        writeBinaryHeader(binaryWriter, graph, 1);
                        writeBinaryOriginBlock(binaryWriter, graph, origin.getStopId(), originCandidates);
                    }
                }
            }
            System.out.printf(
                    Locale.ROOT,
                    "BusTransferStaticGraphCacheJob: wrote %,d rows to %s%n",
                    rows,
                    binaryOutputDir.toAbsolutePath()
            );
        }
    }

    private static void writeBinaryHeader(DataOutputStream writer, Graph graph, int originCount) throws Exception {
        writer.writeInt(BINARY_MAGIC);
        writer.writeInt(BINARY_VERSION);
        writeStringDictionary(writer, graph.stopIds);
        writeStringDictionary(writer, graph.routeIds);
        writeStringDictionary(writer, graph.routeNumbers);
        writer.writeInt(originCount);
    }

    private static void writeStringDictionary(DataOutputStream writer, List<String> values) throws Exception {
        writer.writeInt(values.size());
        for (String value : values) {
            writer.writeUTF(value == null ? "" : value);
        }
    }

    private static void writeBinaryOriginBlock(
            DataOutputStream writer,
            Graph graph,
            String originStopId,
            List<BinaryCandidate> candidates
    ) throws Exception {
        writer.writeInt(graph.stopIdToInt.get(originStopId));
        writer.writeInt(candidates.size());
        for (BinaryCandidate candidate : candidates) {
            writer.writeInt(graph.stopIdToInt.get(candidate.destinationStopId));
            writer.writeInt(candidate.label.rides);
            writer.writeInt((int) Math.round(candidate.label.distanceMeters));
            writer.writeInt(candidate.alternativeIndex);
            List<PathEdge> edges = pathEdges(candidate.label);
            writer.writeInt(edges.size());
            for (PathEdge edge : edges) {
                if (edge instanceof WalkEdge) {
                    WalkEdge walk = (WalkEdge) edge;
                    writer.writeByte(0);
                    writer.writeInt(graph.stopIdToInt.get(walk.startStopId));
                    writer.writeInt(graph.stopIdToInt.get(walk.endStopId));
                    writer.writeInt((int) Math.round(walk.distanceMeters));
                    continue;
                }
                RideEdge ride = (RideEdge) edge;
                writer.writeByte(1);
                writer.writeInt(graph.stopIdToInt.get(ride.startStopId));
                writer.writeInt(graph.stopIdToInt.get(ride.endStopId));
                writer.writeInt((int) Math.round(ride.distanceMeters));
                writer.writeInt(graph.routeIdToInt.get(ride.internalRouteId));
                writer.writeInt(graph.routeNumberToInt.get(ride.routeNumber));
                writer.writeInt(ride.direction);
            }
        }
    }

    private static List<PathEdge> pathEdges(Label label) {
        List<PathEdge> edges = new ArrayList<>();
        for (Label cursor = label; cursor != null && cursor.edge != null; cursor = cursor.previous) {
            edges.add(0, cursor.edge);
        }
        return edges;
    }

    private static Map<String, List<Label>> shortestPathAlternatives(
            Graph graph,
            String originStopId,
            int maxRides,
            int maxSettledStates,
            int maxAlternativesPerState,
            int maxAlternativesPerDestination,
            double alternativeDistanceRatio
    ) {
        PriorityQueue<Label> queue = new PriorityQueue<>(LABEL_ORDER);
        Map<StateKey, List<Label>> labelsByState = new HashMap<>();
        Map<String, List<Label>> labelsByDestination = new HashMap<>();
        Label start = new Label(originStopId, 0, 0.0, null, null);
        queue.add(start);
        labelsByState.put(new StateKey(originStopId, 0), new ArrayList<>(List.of(start)));
        int settled = 0;

        while (!queue.isEmpty() && settled < maxSettledStates) {
            Label current = queue.poll();
            StateKey currentKey = new StateKey(current.stopId, current.rides);
            if (!labelsByState.getOrDefault(currentKey, List.of()).contains(current)) {
                continue;
            }
            settled++;
            if (current.rides > 0) {
                addDestinationLabel(
                        labelsByDestination,
                        current.stopId,
                        current,
                        maxAlternativesPerDestination,
                        alternativeDistanceRatio
                );
            }

            if (current.rides < maxRides) {
                for (RideEdge edge : graph.rideEdgesByStart.getOrDefault(current.stopId, List.of())) {
                    if (current.containsStop(edge.endStopId)) {
                        continue;
                    }
                    addStateLabel(queue, labelsByState, new Label(
                            edge.endStopId,
                            current.rides + 1,
                            current.distanceMeters + edge.distanceMeters,
                            current,
                            edge
                    ), maxAlternativesPerState, alternativeDistanceRatio);
                }
            }
            for (WalkEdge edge : graph.walkEdgesByStart.getOrDefault(current.stopId, List.of())) {
                if (current.containsStop(edge.endStopId)) {
                    continue;
                }
                addStateLabel(queue, labelsByState, new Label(
                        edge.endStopId,
                        current.rides,
                        current.distanceMeters + edge.distanceMeters,
                        current,
                        edge
                ), maxAlternativesPerState, alternativeDistanceRatio);
            }
        }
        labelsByDestination.replaceAll((stopId, labels) -> filteredAlternatives(
                labels,
                maxAlternativesPerDestination,
                alternativeDistanceRatio
        ));
        return labelsByDestination;
    }

    private static List<Label> destinationAlternatives(
            Graph graph,
            Map<String, List<Label>> rawBestByDestination,
            RouteTopology.StopInfo destination,
            int maxAlternatives,
            double alternativeDistanceRatio
    ) {
        List<Label> alternatives = new ArrayList<>();
        for (ClusterStop clusterStop : graph.clusterStopsByStopId.getOrDefault(destination.getStopId(), List.of())) {
            List<Label> labels = rawBestByDestination.get(clusterStop.stopId);
            if (labels == null || labels.isEmpty()) {
                continue;
            }
            for (Label label : labels) {
                Label candidate = label;
                if (!clusterStop.stopId.equals(destination.getStopId())) {
                    candidate = new Label(
                            destination.getStopId(),
                            label.rides,
                            label.distanceMeters + clusterStop.distanceMeters,
                            label,
                            new WalkEdge(
                                    clusterStop.stopId,
                                    destination.getStopId(),
                                    clusterStop.stopName,
                                    destination.getStopName(),
                                    clusterStop.distanceMeters
                            )
                    );
                }
                if (isUsefulAlternative(alternatives, candidate, alternativeDistanceRatio)) {
                    alternatives.add(candidate);
                    alternatives.sort(LABEL_ORDER);
                    trimAlternatives(alternatives, maxAlternatives, alternativeDistanceRatio);
                }
            }
        }
        return filteredAlternatives(alternatives, maxAlternatives, alternativeDistanceRatio);
    }

    private static void addStateLabel(
            PriorityQueue<Label> queue,
            Map<StateKey, List<Label>> labelsByState,
            Label next,
            int maxAlternatives,
            double alternativeDistanceRatio
    ) {
        StateKey key = new StateKey(next.stopId, next.rides);
        List<Label> labels = labelsByState.computeIfAbsent(key, ignored -> new ArrayList<>());
        if (!isUsefulAlternative(labels, next, alternativeDistanceRatio)) {
            return;
        }
        labels.add(next);
        labels.sort(LABEL_ORDER);
        trimAlternatives(labels, maxAlternatives, alternativeDistanceRatio);
        if (!labels.contains(next)) {
            return;
        }
        queue.add(next);
    }

    private static void addDestinationLabel(
            Map<String, List<Label>> labelsByDestination,
            String stopId,
            Label next,
            int maxAlternatives,
            double alternativeDistanceRatio
    ) {
        List<Label> labels = labelsByDestination.computeIfAbsent(stopId, ignored -> new ArrayList<>());
        if (!isUsefulAlternative(labels, next, alternativeDistanceRatio)) {
            return;
        }
        labels.add(next);
        labels.sort(LABEL_ORDER);
        trimAlternatives(labels, maxAlternatives, alternativeDistanceRatio);
    }

    private static boolean isUsefulAlternative(List<Label> existing, Label next, double alternativeDistanceRatio) {
        String nextSignature = next.pathSignature();
        for (Label label : existing) {
            if (label.pathSignature().equals(nextSignature)) {
                return false;
            }
        }
        double bestDistance = existing.stream()
                .mapToDouble(label -> label.distanceMeters)
                .min()
                .orElse(Double.POSITIVE_INFINITY);
        int bestRides = existing.stream()
                .mapToInt(label -> label.rides)
                .min()
                .orElse(Integer.MAX_VALUE);
        return bestDistance == Double.POSITIVE_INFINITY
                || next.distanceMeters <= bestDistance * alternativeDistanceRatio + 0.001
                || next.rides <= bestRides;
    }

    private static List<Label> filteredAlternatives(
            List<Label> labels,
            int maxAlternatives,
            double alternativeDistanceRatio
    ) {
        List<Label> copy = new ArrayList<>(labels);
        copy.sort(LABEL_ORDER);
        trimAlternatives(copy, maxAlternatives, alternativeDistanceRatio);
        return copy;
    }

    private static void trimAlternatives(List<Label> labels, int maxAlternatives, double alternativeDistanceRatio) {
        if (labels.isEmpty()) {
            return;
        }
        double bestDistance = labels.stream().mapToDouble(label -> label.distanceMeters).min().orElse(Double.POSITIVE_INFINITY);
        int bestRides = labels.stream().mapToInt(label -> label.rides).min().orElse(Integer.MAX_VALUE);
        Set<String> signatures = new HashSet<>();
        List<Label> kept = new ArrayList<>();
        for (Label label : labels) {
            if (!signatures.add(label.pathSignature())) {
                continue;
            }
            if (label.distanceMeters > bestDistance * alternativeDistanceRatio + 0.001 && label.rides > bestRides) {
                continue;
            }
            kept.add(label);
            if (kept.size() >= maxAlternatives) {
                break;
            }
        }
        labels.clear();
        labels.addAll(kept);
    }

    private static Map<String, Object> toRecord(
            RouteTopology.StopInfo origin,
            RouteTopology.StopInfo destination,
            Label label,
            int transferLimit,
            int alternativeIndex
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
        record.put("alternativeIndex", alternativeIndex);
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
        String signature();
    }

    private static final class Graph {
        private final Map<String, RouteTopology.StopInfo> stopsById;
        private final List<RouteTopology.StopInfo> sortedStops;
        private final Map<String, List<RideEdge>> rideEdgesByStart;
        private final Map<String, List<WalkEdge>> walkEdgesByStart;
        private final Map<String, List<ClusterStop>> clusterStopsByStopId;
        private final List<String> stopIds;
        private final Map<String, Integer> stopIdToInt;
        private final List<String> routeIds;
        private final Map<String, Integer> routeIdToInt;
        private final List<String> routeNumbers;
        private final Map<String, Integer> routeNumberToInt;

        private Graph(
                Map<String, RouteTopology.StopInfo> stopsById,
                List<RouteTopology.StopInfo> sortedStops,
                Map<String, List<RideEdge>> rideEdgesByStart,
                Map<String, List<WalkEdge>> walkEdgesByStart,
                Map<String, List<ClusterStop>> clusterStopsByStopId
        ) {
            this.stopsById = stopsById;
            this.sortedStops = sortedStops;
            this.rideEdgesByStart = rideEdgesByStart;
            this.walkEdgesByStart = walkEdgesByStart;
            this.clusterStopsByStopId = clusterStopsByStopId;
            this.stopIds = sortedStops.stream()
                    .map(RouteTopology.StopInfo::getStopId)
                    .collect(Collectors.toList());
            this.stopIdToInt = indexByValue(stopIds);
            this.routeIds = rideEdgesByStart.values().stream()
                    .flatMap(Collection::stream)
                    .map(edge -> edge.internalRouteId)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            this.routeIdToInt = indexByValue(routeIds);
            this.routeNumbers = rideEdgesByStart.values().stream()
                    .flatMap(Collection::stream)
                    .map(edge -> edge.routeNumber)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            this.routeNumberToInt = indexByValue(routeNumbers);
        }

        private static Graph build(RouteTopology topology, double walkRadiusMeters, double stopClusterRadiusMeters) {
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
            return new Graph(
                    stopsById,
                    sortedStops,
                    rideEdgesByStart,
                    walkEdgesByStart,
                    buildSameNameClusters(sortedStops, stopClusterRadiusMeters)
            );
        }

        private static Map<String, List<ClusterStop>> buildSameNameClusters(
                List<RouteTopology.StopInfo> stops,
                double stopClusterRadiusMeters
        ) {
            Map<String, List<RouteTopology.StopInfo>> byName = stops.stream()
                    .collect(Collectors.groupingBy(stop -> normalizeStopName(stop.getStopName())));
            Map<String, List<ClusterStop>> result = new HashMap<>();
            for (List<RouteTopology.StopInfo> sameNameStops : byName.values()) {
                for (RouteTopology.StopInfo destination : sameNameStops) {
                    List<ClusterStop> cluster = new ArrayList<>();
                    for (RouteTopology.StopInfo candidate : sameNameStops) {
                        double distance = haversineMeters(
                                destination.getLatitude(),
                                destination.getLongitude(),
                                candidate.getLatitude(),
                                candidate.getLongitude()
                        );
                        if (destination.getStopId().equals(candidate.getStopId()) || distance <= stopClusterRadiusMeters) {
                            cluster.add(new ClusterStop(
                                    candidate.getStopId(),
                                    candidate.getStopName(),
                                    distance
                            ));
                        }
                    }
                    cluster.sort(Comparator
                            .comparingDouble((ClusterStop stop) -> stop.distanceMeters)
                            .thenComparing(stop -> stop.stopId));
                    result.put(destination.getStopId(), cluster);
                }
            }
            return result;
        }
    }

    private static Map<String, Integer> indexByValue(List<String> values) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < values.size(); i++) {
            result.put(values.get(i), i);
        }
        return result;
    }

    private static String normalizeStopName(String stopName) {
        return stopName == null ? "" : stopName.trim().toLowerCase(Locale.ROOT);
    }

    private static final class BinaryCandidate {
        private final String destinationStopId;
        private final Label label;
        private final int alternativeIndex;

        private BinaryCandidate(String destinationStopId, Label label, int alternativeIndex) {
            this.destinationStopId = destinationStopId;
            this.label = label;
            this.alternativeIndex = alternativeIndex;
        }
    }

    private static final class ClusterStop {
        private final String stopId;
        private final String stopName;
        private final double distanceMeters;

        private ClusterStop(String stopId, String stopName, double distanceMeters) {
            this.stopId = stopId;
            this.stopName = stopName;
            this.distanceMeters = distanceMeters;
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

        @Override
        public String signature() {
            return "ride:" + internalRouteId + ":" + direction + ":" + startStopId + ":" + endStopId;
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

        @Override
        public String signature() {
            return "walk:" + startStopId + ":" + endStopId;
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

        private boolean containsStop(String candidateStopId) {
            for (Label cursor = this; cursor != null; cursor = cursor.previous) {
                if (cursor.stopId.equals(candidateStopId)) {
                    return true;
                }
            }
            return false;
        }

        private String pathSignature() {
            List<String> parts = new ArrayList<>();
            for (Label cursor = this; cursor != null && cursor.edge != null; cursor = cursor.previous) {
                parts.add(0, cursor.edge.signature());
            }
            return String.join(";", parts);
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
