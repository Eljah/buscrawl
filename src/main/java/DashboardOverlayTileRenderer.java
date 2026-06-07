import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public final class DashboardOverlayTileRenderer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final int TILE_SIZE = 256;

    private DashboardOverlayTileRenderer() {
    }

    public static void renderTraceTiles(Path traceCacheFile, Path mapConfigFile, Path tilesRoot) throws IOException {
        if (!Files.exists(traceCacheFile) || !Files.exists(mapConfigFile)) {
            return;
        }

        Map<String, Object> tracePayload = MAPPER.readValue(traceCacheFile.toFile(), MAP_TYPE);
        MapConfig config = loadMapConfig(mapConfigFile);
        Path variantRoot = tilesRoot.resolve("traces").resolve("current");
        if (!"ok".equals(String.valueOf(tracePayload.get("status")))) {
            clearDirectory(variantRoot);
            return;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> buses = (List<Map<String, Object>>) tracePayload.getOrDefault("buses", List.of());
        renderVariant(
                variantRoot,
                config,
                graphics -> drawTraceOverlay(graphics, config, buses)
        );
    }

    public static void renderStopLastPassTiles(Path cacheFile, Path mapConfigFile, Path tilesRoot) throws IOException {
        if (!Files.exists(cacheFile) || !Files.exists(mapConfigFile)) {
            return;
        }

        Map<String, Object> payload = MAPPER.readValue(cacheFile.toFile(), MAP_TYPE);
        MapConfig config = loadMapConfig(mapConfigFile);
        Path variantRoot = tilesRoot.resolve("stop-last-pass");
        if (!"ok".equals(String.valueOf(payload.get("status")))) {
            clearDirectory(variantRoot);
            return;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> routeShapes = (List<Map<String, Object>>) payload.getOrDefault("routeShapes", List.of());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> stops = (List<Map<String, Object>>) payload.getOrDefault("stops", List.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> routes = (Map<String, Object>) payload.getOrDefault("routes", Map.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> stopsData = (Map<String, Object>) payload.getOrDefault("stopsData", Map.of());

        Map<String, List<Map<String, Object>>> routeVariants = new LinkedHashMap<>();
        routeVariants.put("routes/previous", castList(routes.get("previousDay")));
        routeVariants.put("routes/all", castList(routes.get("allDaysAverage")));
        routeVariants.putAll(extractWeekdayVariants("routes/weekday", routes.get("weekdayAverage")));

        Map<String, List<Map<String, Object>>> stopVariants = new LinkedHashMap<>();
        stopVariants.put("stops/previous", castList(stopsData.get("previousDay")));
        stopVariants.put("stops/all", castList(stopsData.get("allDaysAverage")));
        stopVariants.putAll(extractWeekdayVariants("stops/weekday", stopsData.get("weekdayAverage")));

        for (Map.Entry<String, List<Map<String, Object>>> entry : routeVariants.entrySet()) {
            Set<String> routeNumbers = new TreeSet<>();
            for (Map<String, Object> row : entry.getValue()) {
                String routeNumber = stringValue(row.get("routeNumber"));
                if (routeNumber != null) {
                    routeNumbers.add(routeNumber);
                }
            }
            Path path = variantRoot.resolve(entry.getKey());
            renderVariant(path, config, graphics -> drawRouteOverlay(graphics, config, routeShapes, routeNumbers));
        }

        for (Map.Entry<String, List<Map<String, Object>>> entry : stopVariants.entrySet()) {
            Set<String> stopIds = new TreeSet<>();
            for (Map<String, Object> row : entry.getValue()) {
                String stopId = stringValue(row.get("stopId"));
                if (stopId != null) {
                    stopIds.add(stopId);
                }
            }
            Path path = variantRoot.resolve(entry.getKey());
            renderVariant(path, config, graphics -> drawStopOverlay(graphics, config, stops, stopIds));
        }
    }

    public static void renderOvertakePointHeatmapTiles(Path cacheFile, Path mapConfigFile, Path tilesRoot) throws IOException {
        if (!Files.exists(cacheFile) || !Files.exists(mapConfigFile)) {
            return;
        }

        Map<String, Object> payload = MAPPER.readValue(cacheFile.toFile(), MAP_TYPE);
        MapConfig config = loadMapConfig(mapConfigFile);
        Path variantRoot = tilesRoot.resolve("overtakes").resolve("point-heatmap");
        if (!"ok".equals(String.valueOf(payload.get("status")))) {
            clearDirectory(variantRoot);
            return;
        }

        renderPointHeatmapScope(variantRoot.resolve("route"), config, payload.get("pointHeatmapData"));
        renderPointHeatmapScope(variantRoot.resolve("physical"), config, payload.get("physicalPointHeatmapData"));
    }

    @SuppressWarnings("unchecked")
    private static void renderPointHeatmapScope(Path scopeRoot, MapConfig config, Object sectionObject) throws IOException {
        if (!(sectionObject instanceof Map<?, ?>)) {
            clearDirectory(scopeRoot);
            return;
        }

        Map<String, Object> section = (Map<String, Object>) sectionObject;
        Map<String, List<Map<String, Object>>> variants = new LinkedHashMap<>();
        variants.put("previous", castList(section.get("previousDay")));
        variants.put("all", castList(section.get("allDays")));
        variants.putAll(extractWeekdayVariants("weekday", section.get("weekday")));

        for (Map.Entry<String, List<Map<String, Object>>> entry : variants.entrySet()) {
            Path path = scopeRoot.resolve(entry.getKey());
            renderPointHeatmapVariant(path, config, entry.getValue());
        }
    }

    private static void renderPointHeatmapVariant(Path outputDir, MapConfig config, List<Map<String, Object>> points) throws IOException {
        Files.createDirectories(outputDir.getParent());
        Path tempRoot = Files.createTempDirectory(outputDir.getParent(), outputDir.getFileName().toString() + "-tmp-");
        try {
            for (int zoom = config.minZoom; zoom <= config.maxZoom; zoom++) {
                Map<String, BufferedImage> images = new HashMap<>();
                for (Map<String, Object> point : points) {
                    Double latitude = doubleValue(point.get("pointLatitude"));
                    Double longitude = doubleValue(point.get("pointLongitude"));
                    if (latitude == null || longitude == null) {
                        continue;
                    }
                    int tileX = (int) Math.floor(lonToWorldX(longitude, zoom) / TILE_SIZE);
                    int tileY = (int) Math.floor(latToWorldY(latitude, zoom) / TILE_SIZE);
                    String key = tileX + "/" + tileY;
                    BufferedImage image = images.computeIfAbsent(key, ignored -> new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB));
                    Graphics2D graphics = image.createGraphics();
                    try {
                        graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);
                        drawOvertakePoint(graphics, zoom, tileX, tileY, point);
                    } finally {
                        graphics.dispose();
                    }
                }

                for (Map.Entry<String, BufferedImage> entry : images.entrySet()) {
                    String[] parts = entry.getKey().split("/");
                    Path xDir = tempRoot.resolve(String.valueOf(zoom)).resolve(parts[0]);
                    Files.createDirectories(xDir);
                    ImageIO.write(entry.getValue(), "PNG", xDir.resolve(parts[1] + ".png").toFile());
                }
            }

            clearDirectory(outputDir);
            Files.createDirectories(outputDir.getParent());
            Files.move(tempRoot, outputDir, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            clearDirectory(tempRoot);
            throw e;
        }
    }

    private static void renderVariant(Path outputDir, MapConfig config, TilePainter painter) throws IOException {
        Files.createDirectories(outputDir.getParent());
        Path tempRoot = Files.createTempDirectory(outputDir.getParent(), outputDir.getFileName().toString() + "-tmp-");
        try {
            for (int zoom = config.minZoom; zoom <= config.maxZoom; zoom++) {
                int minTileX = lonToTileX(config.bounds.west, zoom);
                int maxTileX = lonToTileX(config.bounds.east, zoom);
                int minTileY = latToTileY(config.bounds.north, zoom);
                int maxTileY = latToTileY(config.bounds.south, zoom);

                for (int tileX = minTileX; tileX <= maxTileX; tileX++) {
                    Path xDir = tempRoot.resolve(String.valueOf(zoom)).resolve(String.valueOf(tileX));
                    Files.createDirectories(xDir);
                    for (int tileY = minTileY; tileY <= maxTileY; tileY++) {
                        BufferedImage image = new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
                        Graphics2D graphics = image.createGraphics();
                        try {
                            graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                            painter.paint(new TileGraphics(graphics, zoom, tileX, tileY));
                        } finally {
                            graphics.dispose();
                        }
                        ImageIO.write(image, "PNG", xDir.resolve(tileY + ".png").toFile());
                    }
                }
            }

            clearDirectory(outputDir);
            Files.createDirectories(outputDir.getParent());
            Files.move(tempRoot, outputDir, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            clearDirectory(tempRoot);
            throw e;
        }
    }

    private static void drawTraceOverlay(TileGraphics tileGraphics, MapConfig config, List<Map<String, Object>> buses) {
        Graphics2D graphics = tileGraphics.graphics;
        for (Map<String, Object> bus : buses) {
            String routeNumber = stringValue(bus.get("routeNumber"));
            String plate = stringValue(bus.get("plate"));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> points = (List<Map<String, Object>>) bus.getOrDefault("points", List.of());
            Color baseColor = traceColor((routeNumber == null ? "" : routeNumber) + ":" + (plate == null ? "" : plate));

            for (int i = 0; i < points.size(); i++) {
                Map<String, Object> point = points.get(i);
                Double latitude = doubleValue(point.get("latitude"));
                Double longitude = doubleValue(point.get("longitude"));
                if (latitude == null || longitude == null) {
                    continue;
                }
                double fade = points.size() <= 1 ? 1.0 : (double) i / (double) (points.size() - 1);
                double radius = 1.5 + fade * 3.4;
                int alpha = Math.max(10, Math.min(255, (int) Math.round((0.03 + fade * 0.92) * 255.0)));
                graphics.setColor(new Color(baseColor.getRed(), baseColor.getGreen(), baseColor.getBlue(), alpha));
                double x = lonToWorldX(longitude, tileGraphics.zoom) - tileGraphics.tileOriginX;
                double y = latToWorldY(latitude, tileGraphics.zoom) - tileGraphics.tileOriginY;
                graphics.fillOval(
                        (int) Math.round(x - radius),
                        (int) Math.round(y - radius),
                        (int) Math.round(radius * 2.0),
                        (int) Math.round(radius * 2.0)
                );
            }

            Double latestLatitude = doubleValue(bus.get("latestLatitude"));
            Double latestLongitude = doubleValue(bus.get("latestLongitude"));
            if (latestLatitude != null && latestLongitude != null) {
                double x = lonToWorldX(latestLongitude, tileGraphics.zoom) - tileGraphics.tileOriginX;
                double y = latToWorldY(latestLatitude, tileGraphics.zoom) - tileGraphics.tileOriginY;
                graphics.setColor(new Color(baseColor.getRed(), baseColor.getGreen(), baseColor.getBlue(), 240));
                graphics.fillOval((int) Math.round(x - 4), (int) Math.round(y - 4), 8, 8);
            }
        }
    }

    private static void drawRouteOverlay(
            TileGraphics tileGraphics,
            MapConfig config,
            List<Map<String, Object>> routeShapes,
            Set<String> routeNumbers
    ) {
        Graphics2D graphics = tileGraphics.graphics;
        graphics.setColor(new Color(29, 42, 56, 72));
        graphics.setStroke(new BasicStroke(routeStrokeWidth(tileGraphics.zoom), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));

        for (Map<String, Object> route : routeShapes) {
            String routeNumber = stringValue(route.get("routeNumber"));
            if (routeNumber == null || !routeNumbers.contains(routeNumber)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            List<List<Map<String, Object>>> paths = (List<List<Map<String, Object>>>) route.getOrDefault("paths", List.of());
            for (List<Map<String, Object>> path : paths) {
                Path2D polyline = new Path2D.Double();
                boolean started = false;
                for (Map<String, Object> point : path) {
                    Double latitude = doubleValue(point.get("latitude"));
                    Double longitude = doubleValue(point.get("longitude"));
                    if (latitude == null || longitude == null) {
                        continue;
                    }
                    double x = lonToWorldX(longitude, tileGraphics.zoom) - tileGraphics.tileOriginX;
                    double y = latToWorldY(latitude, tileGraphics.zoom) - tileGraphics.tileOriginY;
                    if (!started) {
                        polyline.moveTo(x, y);
                        started = true;
                    } else {
                        polyline.lineTo(x, y);
                    }
                }
                if (started) {
                    graphics.draw(polyline);
                }
            }
        }
    }

    private static void drawStopOverlay(
            TileGraphics tileGraphics,
            MapConfig config,
            List<Map<String, Object>> stops,
            Set<String> stopIds
    ) {
        Graphics2D graphics = tileGraphics.graphics;
        graphics.setColor(new Color(13, 107, 102, 96));
        double radius = stopRadius(tileGraphics.zoom);
        for (Map<String, Object> stop : stops) {
            String stopId = stringValue(stop.get("stopId"));
            if (stopId == null || !stopIds.contains(stopId)) {
                continue;
            }
            Double latitude = doubleValue(stop.get("latitude"));
            Double longitude = doubleValue(stop.get("longitude"));
            if (latitude == null || longitude == null) {
                continue;
            }
            double x = lonToWorldX(longitude, tileGraphics.zoom) - tileGraphics.tileOriginX;
            double y = latToWorldY(latitude, tileGraphics.zoom) - tileGraphics.tileOriginY;
            graphics.fillOval(
                    (int) Math.round(x - radius),
                    (int) Math.round(y - radius),
                    (int) Math.round(radius * 2.0),
                    (int) Math.round(radius * 2.0)
            );
        }
    }

    private static void drawOvertakePointHeatmap(TileGraphics tileGraphics, List<Map<String, Object>> points) {
        Graphics2D graphics = tileGraphics.graphics;
        graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_OFF);
        for (Map<String, Object> point : points) {
            drawOvertakePoint(graphics, tileGraphics.zoom, (int) (tileGraphics.tileOriginX / TILE_SIZE), (int) (tileGraphics.tileOriginY / TILE_SIZE), point);
        }
    }

    private static void drawOvertakePoint(Graphics2D graphics, int zoom, int tileX, int tileY, Map<String, Object> point) {
        Double latitude = doubleValue(point.get("pointLatitude"));
        Double longitude = doubleValue(point.get("pointLongitude"));
        if (latitude == null || longitude == null) {
            return;
        }
        int count = Math.max(1, requireInt(point.getOrDefault("overtakeCount", 1)));
        double x = lonToWorldX(longitude, zoom) - tileX * (double) TILE_SIZE;
        double y = latToWorldY(latitude, zoom) - tileY * (double) TILE_SIZE;
        int left = (int) Math.round(x - 2.0);
        int top = (int) Math.round(y - 2.0);
        int repeats = Math.min(count, 500);
        for (int i = 0; i < repeats; i++) {
            graphics.setColor(new Color(206, 44, 36, 12));
            graphics.fillOval(left, top, 4, 4);
        }
    }

    private static Map<String, List<Map<String, Object>>> extractWeekdayVariants(String prefix, Object source) {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        if (!(source instanceof Map<?, ?>)) {
            return result;
        }
        Map<?, ?> sourceMap = (Map<?, ?>) source;
        for (Map.Entry<?, ?> entry : sourceMap.entrySet()) {
            result.put(prefix + "/" + entry.getKey(), castList(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> castList(Object value) {
        if (value instanceof List<?>) {
            return (List<Map<String, Object>>) value;
        }
        return List.of();
    }

    private static MapConfig loadMapConfig(Path mapConfigFile) throws IOException {
        Map<String, Object> map = MAPPER.readValue(mapConfigFile.toFile(), MAP_TYPE);
        @SuppressWarnings("unchecked")
        Map<String, Object> boundsMap = (Map<String, Object>) map.get("bounds");
        if (boundsMap == null) {
            throw new IOException("Map bounds not found in " + mapConfigFile);
        }
        GeoBounds bounds = new GeoBounds(
                requireDouble(boundsMap.get("west")),
                requireDouble(boundsMap.get("south")),
                requireDouble(boundsMap.get("east")),
                requireDouble(boundsMap.get("north"))
        );
        return new MapConfig(
                bounds,
                requireInt(map.get("minZoom")),
                requireInt(map.get("maxZoom"))
        );
    }

    private static Color traceColor(String key) {
        int hash = key.hashCode();
        float hue = ((hash & Integer.MAX_VALUE) % 360) / 360.0f;
        return Color.getHSBColor(hue, 0.82f, 0.94f);
    }

    private static float routeStrokeWidth(int zoom) {
        return (float) Math.max(1.8, 1.2 + (zoom - 11) * 0.35);
    }

    private static double stopRadius(int zoom) {
        return Math.max(2.8, 2.3 + (zoom - 11) * 0.35);
    }

    private static int lonToTileX(double longitude, int zoom) {
        return (int) Math.floor((longitude + 180.0) / 360.0 * (1 << zoom));
    }

    private static int latToTileY(double latitude, int zoom) {
        double latRad = Math.toRadians(latitude);
        double mercator = Math.log(Math.tan(Math.PI / 4.0 + latRad / 2.0));
        return (int) Math.floor((1.0 - mercator / Math.PI) / 2.0 * (1 << zoom));
    }

    private static double lonToWorldX(double longitude, int zoom) {
        return (longitude + 180.0) / 360.0 * TILE_SIZE * (1 << zoom);
    }

    private static double latToWorldY(double latitude, int zoom) {
        double sin = Math.sin(Math.toRadians(latitude));
        double y = 0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI);
        return y * TILE_SIZE * (1 << zoom);
    }

    private static void clearDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        List<Path> paths = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            stream.forEach(paths::add);
        }
        paths.sort((left, right) -> right.getNameCount() - left.getNameCount());
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }

    private static String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private static Double doubleValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value == null) {
            return null;
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static double requireDouble(Object value) {
        Double parsed = doubleValue(value);
        if (parsed == null) {
            throw new IllegalArgumentException("Expected numeric value, got " + value);
        }
        return parsed;
    }

    private static int requireInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private interface TilePainter {
        void paint(TileGraphics graphics);
    }

    private static final class TileGraphics {
        private final Graphics2D graphics;
        private final int zoom;
        private final double tileOriginX;
        private final double tileOriginY;

        private TileGraphics(Graphics2D graphics, int zoom, int tileX, int tileY) {
            this.graphics = Objects.requireNonNull(graphics);
            this.zoom = zoom;
            this.tileOriginX = tileX * (double) TILE_SIZE;
            this.tileOriginY = tileY * (double) TILE_SIZE;
        }
    }

    private static final class MapConfig {
        private final GeoBounds bounds;
        private final int minZoom;
        private final int maxZoom;

        private MapConfig(GeoBounds bounds, int minZoom, int maxZoom) {
            this.bounds = bounds;
            this.minZoom = minZoom;
            this.maxZoom = maxZoom;
        }
    }

    private static final class GeoBounds {
        private final double west;
        private final double south;
        private final double east;
        private final double north;

        private GeoBounds(double west, double south, double east, double north) {
            this.west = west;
            this.south = south;
            this.east = east;
            this.north = north;
        }
    }
}
