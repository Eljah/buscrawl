import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.imageio.ImageIO;
import javax.xml.parsers.DocumentBuilderFactory;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BusOsmTileGenerator {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OkHttpClient HTTP = new OkHttpClient.Builder().build();
    private static final int TILE_SIZE = 256;

    public static void main(String[] args) throws Exception {
        Path tilesRoot = Path.of(System.getenv().getOrDefault(
                "BUS_TILE_DIR",
                "./var/bus/dashboard-cache/tiles/base"
        ));
        Path mapConfigFile = Path.of(System.getenv().getOrDefault(
                "BUS_DASHBOARD_MAP_CONFIG_FILE",
                "./var/bus/dashboard-cache/map-config.json"
        ));
        int minZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_MAP_MIN_ZOOM", "11"));
        int maxZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_MAP_MAX_ZOOM", "17"));
        double paddingFactor = Double.parseDouble(System.getenv().getOrDefault("BUS_MAP_PADDING_FACTOR", "0.20"));
        String overpassUrl = System.getenv().getOrDefault(
                "BUS_OVERPASS_URL",
                "https://overpass-api.de/api/interpreter"
        );
        String osmApiUrl = System.getenv().getOrDefault(
                "BUS_OSM_API_URL",
                "https://api.openstreetmap.org/api/0.6/map"
        );

        Files.createDirectories(tilesRoot);
        Files.createDirectories(mapConfigFile.getParent());

        GeoBounds bounds = loadBoundsFromStops().pad(paddingFactor);
        OsmData osmData = loadOsmData(overpassUrl, osmApiUrl, bounds);
        renderTiles(osmData, bounds, tilesRoot, minZoom, maxZoom);
        writeJsonAtomic(mapConfigFile, buildConfigPayload(bounds, minZoom, maxZoom));
    }

    private static GeoBounds loadBoundsFromStops() throws IOException {
        try (InputStream inputStream = BusOsmTileGenerator.class.getClassLoader().getResourceAsStream("routes.json")) {
            if (inputStream == null) {
                throw new IOException("routes.json not found");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> root = MAPPER.readValue(inputStream, Map.class);
            Object stopsSection = root.get("nbusstop");
            if (!(stopsSection instanceof Map<?, ?>)) {
                throw new IOException("nbusstop section not found in routes.json");
            }

            List<Double> longitudes = new ArrayList<>();
            List<Double> latitudes = new ArrayList<>();
            for (Object value : ((Map<?, ?>) stopsSection).values()) {
                if (!(value instanceof List<?>)) {
                    continue;
                }
                List<?> stop = (List<?>) value;
                if (stop.size() < 3) {
                    continue;
                }
                if (!(stop.get(1) instanceof Number) || !(stop.get(2) instanceof Number)) {
                    continue;
                }
                double longitude = ((Number) stop.get(1)).doubleValue();
                double latitude = ((Number) stop.get(2)).doubleValue();
                if (latitude < 54.5 || latitude > 56.5 || longitude < 48.0 || longitude > 51.0) {
                    continue;
                }
                longitudes.add(longitude);
                latitudes.add(latitude);
            }

            if (longitudes.isEmpty() || latitudes.isEmpty()) {
                throw new IOException("No stop coordinates found for bounds calculation");
            }

            longitudes.sort(Comparator.naturalOrder());
            latitudes.sort(Comparator.naturalOrder());

            double west = percentile(longitudes, 0.02);
            double east = percentile(longitudes, 0.98);
            double south = percentile(latitudes, 0.02);
            double north = percentile(latitudes, 0.98);
            return new GeoBounds(west, south, east, north);
        }
    }

    private static OsmData loadOsmData(String overpassUrl, String osmApiUrl, GeoBounds bounds) throws Exception {
        String bbox = String.format(
                Locale.US,
                "%.6f,%.6f,%.6f,%.6f",
                bounds.south,
                bounds.west,
                bounds.north,
                bounds.east
        );

        String query = "[out:xml][timeout:180];(" +
                "way[\"highway\"~\"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link|unclassified|residential|service|living_street|road\"](" + bbox + ");" +
                "way[\"railway\"~\"rail|light_rail|subway|tram|narrow_gauge\"](" + bbox + ");" +
                ");(._;>;);out body;";

        Request request = new Request.Builder()
                .url(overpassUrl)
                .post(new FormBody.Builder()
                        .add("data", query)
                        .build())
                .build();

        try (Response response = HTTP.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                return parseOsmXml(response.body().bytes());
            }
        }

        return loadOsmDataFromMainApi(osmApiUrl, bounds);
    }

    private static OsmData loadOsmDataFromMainApi(String osmApiUrl, GeoBounds bounds) throws Exception {
        double chunkSize = Double.parseDouble(System.getenv().getOrDefault("BUS_OSM_CHUNK_SIZE_DEGREES", "0.02"));
        Map<Long, WayFeature> ways = new LinkedHashMap<>();

        for (double west = bounds.west; west < bounds.east; west += chunkSize) {
            double east = Math.min(bounds.east, west + chunkSize);
            for (double south = bounds.south; south < bounds.north; south += chunkSize) {
                double north = Math.min(bounds.north, south + chunkSize);
                String bbox = String.format(Locale.US, "%.6f,%.6f,%.6f,%.6f", west, south, east, north);
                Request request = new Request.Builder()
                        .url(osmApiUrl + "?bbox=" + bbox)
                        .build();

                try (Response response = HTTP.newCall(request).execute()) {
                    if (!response.isSuccessful() || response.body() == null) {
                        throw new IOException("OSM map API request failed: " + response);
                    }
                    parseOsmXmlInto(response.body().bytes(), ways);
                }
            }
        }

        return new OsmData(new ArrayList<>(ways.values()));
    }

    private static OsmData parseOsmXml(byte[] xmlBytes) throws Exception {
        Map<Long, WayFeature> ways = new LinkedHashMap<>();
        parseOsmXmlInto(xmlBytes, ways);
        return new OsmData(new ArrayList<>(ways.values()));
    }

    private static void parseOsmXmlInto(byte[] xmlBytes, Map<Long, WayFeature> ways) throws Exception {
        Document document = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new ByteArrayInputStream(xmlBytes));
        document.getDocumentElement().normalize();

        Map<Long, GeoPoint> nodes = new HashMap<>();
        NodeList nodeList = document.getElementsByTagName("node");
        for (int i = 0; i < nodeList.getLength(); i++) {
            Element element = (Element) nodeList.item(i);
            long id = Long.parseLong(element.getAttribute("id"));
            double latitude = Double.parseDouble(element.getAttribute("lat"));
            double longitude = Double.parseDouble(element.getAttribute("lon"));
            nodes.put(id, new GeoPoint(longitude, latitude));
        }

        NodeList wayList = document.getElementsByTagName("way");
        for (int i = 0; i < wayList.getLength(); i++) {
            Element element = (Element) wayList.item(i);
            long id = Long.parseLong(element.getAttribute("id"));
            String highway = null;
            String railway = null;
            List<Long> refs = new ArrayList<>();

            NodeList children = element.getChildNodes();
            for (int childIndex = 0; childIndex < children.getLength(); childIndex++) {
                Node child = children.item(childIndex);
                if (!(child instanceof Element)) {
                    continue;
                }
                Element childElement = (Element) child;
                if ("nd".equals(childElement.getTagName())) {
                    refs.add(Long.parseLong(childElement.getAttribute("ref")));
                } else if ("tag".equals(childElement.getTagName())) {
                    String key = childElement.getAttribute("k");
                    String value = childElement.getAttribute("v");
                    if ("highway".equals(key)) {
                        highway = value;
                    } else if ("railway".equals(key)) {
                        railway = value;
                    }
                }
            }

            if (highway == null && railway == null) {
                continue;
            }

            List<GeoPoint> geometry = new ArrayList<>();
            for (Long ref : refs) {
                GeoPoint point = nodes.get(ref);
                if (point != null) {
                    geometry.add(point);
                }
            }
            if (geometry.size() >= 2) {
                ways.put(id, new WayFeature(id, highway, railway, geometry));
            }
        }
    }

    private static void renderTiles(OsmData osmData, GeoBounds bounds, Path tilesRoot, int minZoom, int maxZoom) throws IOException {
        for (int zoom = minZoom; zoom <= maxZoom; zoom++) {
            int minTileX = lonToTileX(bounds.west, zoom);
            int maxTileX = lonToTileX(bounds.east, zoom);
            int minTileY = latToTileY(bounds.north, zoom);
            int maxTileY = latToTileY(bounds.south, zoom);

            for (int tileX = minTileX; tileX <= maxTileX; tileX++) {
                for (int tileY = minTileY; tileY <= maxTileY; tileY++) {
                    BufferedImage image = new BufferedImage(TILE_SIZE, TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
                    Graphics2D graphics = image.createGraphics();
                    try {
                        graphics.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                        graphics.setColor(new Color(109, 114, 119));
                        graphics.fillRect(0, 0, TILE_SIZE, TILE_SIZE);
                        drawWays(graphics, osmData.ways, zoom, tileX, tileY);
                    } finally {
                        graphics.dispose();
                    }

                    Path xDir = tilesRoot.resolve(String.valueOf(zoom)).resolve(String.valueOf(tileX));
                    Files.createDirectories(xDir);
                    ImageIO.write(image, "PNG", xDir.resolve(tileY + ".png").toFile());
                }
            }
        }
    }

    private static void drawWays(Graphics2D graphics, List<WayFeature> ways, int zoom, int tileX, int tileY) {
        double tileOriginX = tileX * TILE_SIZE;
        double tileOriginY = tileY * TILE_SIZE;

        for (WayFeature way : ways) {
            boolean rail = way.railway != null;
            Path2D path = new Path2D.Double();
            boolean started = false;
            for (GeoPoint point : way.geometry) {
                double globalX = lonToWorldX(point.longitude, zoom);
                double globalY = latToWorldY(point.latitude, zoom);
                double localX = globalX - tileOriginX;
                double localY = globalY - tileOriginY;
                if (!started) {
                    path.moveTo(localX, localY);
                    started = true;
                } else {
                    path.lineTo(localX, localY);
                }
            }
            if (!started) {
                continue;
            }

            graphics.setColor(rail ? new Color(20, 20, 20, 220) : new Color(246, 246, 246, 214));
            graphics.setStroke(new BasicStroke(strokeWidth(way, zoom), BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND));
            graphics.draw(path);
        }
    }

    private static float strokeWidth(WayFeature way, int zoom) {
        if (way.railway != null) {
            return (float) Math.max(1.3, 0.9 + (zoom - 11) * 0.25);
        }
        String highway = way.highway == null ? "" : way.highway;
        if (highway.startsWith("motorway") || highway.startsWith("trunk")) {
            return (float) Math.max(1.8, 1.4 + (zoom - 11) * 0.55);
        }
        if (highway.startsWith("primary") || highway.startsWith("secondary")) {
            return (float) Math.max(1.4, 1.1 + (zoom - 11) * 0.45);
        }
        if (highway.startsWith("tertiary")) {
            return (float) Math.max(1.2, 0.9 + (zoom - 11) * 0.35);
        }
        return (float) Math.max(1.0, 0.7 + (zoom - 11) * 0.28);
    }

    private static Map<String, Object> buildConfigPayload(GeoBounds bounds, int minZoom, int maxZoom) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("updatedAt", Instant.now().atOffset(ZoneOffset.UTC).toString());
        payload.put("status", "ok");
        payload.put("tileSize", TILE_SIZE);
        payload.put("minZoom", minZoom);
        payload.put("maxZoom", maxZoom);
        payload.put("initialZoom", Math.min(maxZoom, Math.max(minZoom, 12)));
        payload.put("tileUrlTemplate", "./tiles/base/{z}/{x}/{y}.png");

        Map<String, Object> boundsMap = new LinkedHashMap<>();
        boundsMap.put("west", bounds.west);
        boundsMap.put("south", bounds.south);
        boundsMap.put("east", bounds.east);
        boundsMap.put("north", bounds.north);
        payload.put("bounds", boundsMap);

        Map<String, Object> center = new LinkedHashMap<>();
        center.put("longitude", (bounds.west + bounds.east) / 2.0);
        center.put("latitude", (bounds.south + bounds.north) / 2.0);
        payload.put("center", center);
        return payload;
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

    private static double percentile(List<Double> values, double percentile) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values must not be empty");
        }
        if (values.size() == 1) {
            return values.get(0);
        }
        double position = percentile * (values.size() - 1);
        int lower = (int) Math.floor(position);
        int upper = (int) Math.ceil(position);
        if (lower == upper) {
            return values.get(lower);
        }
        double ratio = position - lower;
        return values.get(lower) + (values.get(upper) - values.get(lower)) * ratio;
    }

    private static void writeJsonAtomic(Path target, Object payload) throws IOException {
        Path tempFile = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), payload);
        Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static final class OsmData {
        private final List<WayFeature> ways;

        private OsmData(List<WayFeature> ways) {
            this.ways = ways;
        }
    }

    private static final class WayFeature {
        private final long id;
        private final String highway;
        private final String railway;
        private final List<GeoPoint> geometry;

        private WayFeature(long id, String highway, String railway, List<GeoPoint> geometry) {
            this.id = id;
            this.highway = highway;
            this.railway = railway;
            this.geometry = geometry;
        }
    }

    private static final class GeoPoint {
        private final double longitude;
        private final double latitude;

        private GeoPoint(double longitude, double latitude) {
            this.longitude = longitude;
            this.latitude = latitude;
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

        private GeoBounds pad(double factor) {
            double width = east - west;
            double height = north - south;
            return new GeoBounds(
                    west - width * factor,
                    south - height * factor,
                    east + width * factor,
                    north + height * factor
            );
        }
    }
}
