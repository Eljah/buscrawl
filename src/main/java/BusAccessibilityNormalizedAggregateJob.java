import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BusAccessibilityNormalizedAggregateJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ZoneId CITY_ZONE = ZoneId.of(System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow"));
    private static final Path ORIGIN_DIR = Path.of(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_AGGREGATE_ORIGIN_DIR",
            "/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-origins"
    ));
    private static final Path OUTPUT_DIR = Path.of(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_AGGREGATE_OUTPUT_DIR",
            "/home/eljah/apps/buscrawl/dashboard-cache/accessibility-map-aggregates"
    ));
    private static final Path TILE_ROOT = Path.of(System.getenv().getOrDefault(
            "BUS_TILE_ROOT",
            "/home/eljah/apps/buscrawl/dashboard-cache/tiles"
    ));
    private static final String AGGREGATE_TILE_PREFIX = System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_AGGREGATE_TILE_PREFIX",
            "accessibility-aggregates"
    );
    private static final YearMonth TARGET_MONTH = YearMonth.parse(System.getenv().getOrDefault(
            "BUS_ACCESSIBILITY_AGGREGATE_MONTH",
            YearMonth.from(LocalDate.now(CITY_ZONE).minusMonths(1)).toString()
    ));

    public static void main(String[] args) throws Exception {
        Files.createDirectories(OUTPUT_DIR);
        Files.createDirectories(TILE_ROOT.resolve(AGGREGATE_TILE_PREFIX));
        List<Path> originFiles;
        try (Stream<Path> stream = Files.list(ORIGIN_DIR)) {
            originFiles = stream
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .collect(Collectors.toList());
        }
        int origins = 0;
        int aggregateSnapshots = 0;
        for (Path originFile : originFiles) {
            OriginResult result = processOrigin(originFile);
            if (result != null) {
                origins++;
                aggregateSnapshots += result.snapshotCount;
            }
        }
        System.out.printf(
                Locale.ROOT,
                "BusAccessibilityNormalizedAggregateJob: processed origins=%d aggregateSnapshots=%d month=%s output=%s%n",
                origins,
                aggregateSnapshots,
                TARGET_MONTH,
                OUTPUT_DIR
        );
    }

    @SuppressWarnings("unchecked")
    private static OriginResult processOrigin(Path originFile) throws Exception {
        Map<String, Object> payload = MAPPER.readValue(originFile.toFile(), new TypeReference<>() {
        });
        String slug = stripJson(originFile.getFileName().toString());
        List<Map<String, Object>> snapshots = asList(payload.get("snapshots"));
        if (snapshots.isEmpty()) {
            return null;
        }
        String tileUrlPrefix = stringValue(payload.get("tileUrlPrefix"), "");
        List<AggregateSnapshot> aggregateSnapshots = new ArrayList<>();
        aggregateSnapshots.addAll(buildMonthlySnapshots(slug, tileUrlPrefix, snapshots));
        aggregateSnapshots.addAll(buildYearSnapshots(slug, TARGET_MONTH.getYear()));
        if (aggregateSnapshots.isEmpty()) {
            return null;
        }
        aggregateSnapshots.sort(Comparator
                .comparing((AggregateSnapshot snapshot) -> snapshot.type)
                .thenComparing(snapshot -> snapshot.periodKey)
                .thenComparingInt(snapshot -> snapshot.weekdayIso)
                .thenComparing(snapshot -> snapshot.departureTime));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", "ok");
        out.put("updatedAt", java.time.Instant.now().toString());
        out.put("originSlug", slug);
        out.put("originStopName", payload.get("originStopName"));
        out.put("tileMinZoom", payload.get("tileMinZoom"));
        out.put("tileMaxZoom", payload.get("tileMaxZoom"));
        out.put("overlayTileMaxZoom", payload.get("overlayTileMaxZoom"));
        out.put("totalNormalizedColorMinMinutes", payload.getOrDefault("totalNormalizedColorMinMinutes", 0));
        out.put("totalNormalizedColorMaxMinutes", payload.getOrDefault("totalNormalizedColorMaxMinutes", 800));
        out.put("aggregateSnapshots", aggregateSnapshots.stream().map(AggregateSnapshot::toPayload).collect(Collectors.toList()));
        writeJsonAtomic(OUTPUT_DIR.resolve(slug + ".json"), out);
        return new OriginResult(aggregateSnapshots.size());
    }

    private static List<AggregateSnapshot> buildMonthlySnapshots(
            String slug,
            String tileUrlPrefix,
            List<Map<String, Object>> snapshots
    ) throws Exception {
        Map<String, List<Map<String, Object>>> groups = new LinkedHashMap<>();
        for (Map<String, Object> snapshot : snapshots) {
            String serviceDateText = stringValue(snapshot.get("serviceDate"), "");
            String departureTime = stringValue(snapshot.get("departureTime"), "");
            String template = stringValue(snapshot.get("totalNormalizedTileOverlayTemplate"), "");
            if (serviceDateText.isBlank() || departureTime.isBlank() || template.isBlank()) {
                continue;
            }
            LocalDate serviceDate = LocalDate.parse(serviceDateText);
            if (!YearMonth.from(serviceDate).equals(TARGET_MONTH)) {
                continue;
            }
            int weekdayIso = serviceDate.getDayOfWeek().getValue();
            groups.computeIfAbsent(weekdayIso + "|" + departureTime, ignored -> new ArrayList<>()).add(snapshot);
        }
        List<AggregateSnapshot> result = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : groups.entrySet()) {
            String[] parts = entry.getKey().split("\\|", -1);
            int weekdayIso = Integer.parseInt(parts[0]);
            String departureTime = parts[1];
            String hhmm = departureTime.replace(":", "");
            Path outputTileRoot = TILE_ROOT.resolve(AGGREGATE_TILE_PREFIX)
                    .resolve(slug)
                    .resolve("month")
                    .resolve(TARGET_MONTH.toString())
                    .resolve("weekday-" + weekdayIso)
                    .resolve(hhmm)
                    .resolve("total-normalized");
            List<Path> sourceRoots = entry.getValue().stream()
                    .map(snapshot -> sourceTileRoot(tileUrlPrefix, snapshot))
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());
            int tileCount = averageTileRoots(sourceRoots, outputTileRoot);
            if (tileCount > 0) {
                result.add(new AggregateSnapshot(
                        "monthWeekday",
                        TARGET_MONTH.toString(),
                        TARGET_MONTH.getYear(),
                        TARGET_MONTH.toString(),
                        weekdayIso,
                        departureTime,
                        entry.getValue().size(),
                        tileCount,
                        "./tiles/" + AGGREGATE_TILE_PREFIX + "/" + slug + "/month/" + TARGET_MONTH + "/weekday-" + weekdayIso + "/" + hhmm + "/total-normalized/{z}/{x}/{y}.png"
                ));
            }
        }
        return result;
    }

    private static List<AggregateSnapshot> buildYearSnapshots(String slug, int year) throws Exception {
        Path monthRoot = TILE_ROOT.resolve(AGGREGATE_TILE_PREFIX).resolve(slug).resolve("month");
        if (!Files.isDirectory(monthRoot)) {
            return List.of();
        }
        Map<String, List<Path>> groups = new LinkedHashMap<>();
        try (Stream<Path> months = Files.list(monthRoot)) {
            for (Path monthPath : months.filter(Files::isDirectory).collect(Collectors.toList())) {
                YearMonth month;
                try {
                    month = YearMonth.parse(monthPath.getFileName().toString());
                } catch (Exception ignored) {
                    continue;
                }
                if (month.getYear() != year) {
                    continue;
                }
                for (int weekday = 1; weekday <= 7; weekday++) {
                    Path weekdayPath = monthPath.resolve("weekday-" + weekday);
                    if (!Files.isDirectory(weekdayPath)) {
                        continue;
                    }
                    try (Stream<Path> times = Files.list(weekdayPath)) {
                        for (Path timePath : times.filter(Files::isDirectory).collect(Collectors.toList())) {
                            Path normalizedRoot = timePath.resolve("total-normalized");
                            if (Files.isDirectory(normalizedRoot)) {
                                groups.computeIfAbsent(weekday + "|" + timePath.getFileName(), ignored -> new ArrayList<>()).add(normalizedRoot);
                            }
                        }
                    }
                }
            }
        }
        List<AggregateSnapshot> result = new ArrayList<>();
        for (Map.Entry<String, List<Path>> entry : groups.entrySet()) {
            String[] parts = entry.getKey().split("\\|", -1);
            int weekdayIso = Integer.parseInt(parts[0]);
            String hhmm = parts[1];
            String departureTime = hhmm.substring(0, 2) + ":" + hhmm.substring(2, 4);
            Path outputTileRoot = TILE_ROOT.resolve(AGGREGATE_TILE_PREFIX)
                    .resolve(slug)
                    .resolve("year")
                    .resolve(String.valueOf(year))
                    .resolve("weekday-" + weekdayIso)
                    .resolve(hhmm)
                    .resolve("total-normalized");
            int tileCount = averageTileRoots(entry.getValue(), outputTileRoot);
            if (tileCount > 0) {
                result.add(new AggregateSnapshot(
                        "yearWeekday",
                        String.valueOf(year),
                        year,
                        null,
                        weekdayIso,
                        departureTime,
                        entry.getValue().size(),
                        tileCount,
                        "./tiles/" + AGGREGATE_TILE_PREFIX + "/" + slug + "/year/" + year + "/weekday-" + weekdayIso + "/" + hhmm + "/total-normalized/{z}/{x}/{y}.png"
                ));
            }
        }
        return result;
    }

    private static Path sourceTileRoot(String tileUrlPrefix, Map<String, Object> snapshot) {
        String template = stringValue(snapshot.get("totalNormalizedTileOverlayTemplate"), "");
        if (!template.isBlank()) {
            String stripped = template.replace("\\", "/");
            stripped = stripped.replace("./tiles/", "");
            int marker = stripped.indexOf("/{z}/{x}/{y}.png");
            if (marker >= 0) {
                return TILE_ROOT.resolve(stripped.substring(0, marker)).normalize();
            }
        }
        return TILE_ROOT.resolve(tileUrlPrefix)
                .resolve(stringValue(snapshot.get("id"), ""))
                .resolve("total-normalized")
                .normalize();
    }

    private static int averageTileRoots(List<Path> sourceRoots, Path outputRoot) throws Exception {
        if (sourceRoots.isEmpty()) {
            return 0;
        }
        Set<String> tileKeys = new TreeSet<>();
        for (Path root : sourceRoots) {
            try (Stream<Path> files = Files.walk(root)) {
                files.filter(path -> path.getFileName().toString().endsWith(".png"))
                        .forEach(path -> tileKeys.add(root.relativize(path).toString().replace('\\', '/')));
            }
        }
        if (tileKeys.isEmpty()) {
            return 0;
        }
        Path tempRoot = outputRoot.resolveSibling(outputRoot.getFileName() + ".tmp-" + System.currentTimeMillis());
        Files.createDirectories(tempRoot);
        int written = 0;
        try {
            for (String tileKey : tileKeys) {
                BufferedImage averaged = averageTile(sourceRoots, tileKey);
                if (averaged == null) {
                    continue;
                }
                Path out = tempRoot.resolve(tileKey);
                Files.createDirectories(out.getParent());
                ImageIO.write(averaged, "png", out.toFile());
                written++;
            }
            if (Files.exists(outputRoot)) {
                clearDirectory(outputRoot);
                Files.delete(outputRoot);
            }
            Files.createDirectories(outputRoot.getParent());
            try {
                Files.move(tempRoot, outputRoot, StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception ignored) {
                Files.move(tempRoot, outputRoot, StandardCopyOption.REPLACE_EXISTING);
            }
            return written;
        } catch (Exception e) {
            clearDirectoryQuietly(tempRoot);
            throw e;
        }
    }

    private static BufferedImage averageTile(List<Path> roots, String tileKey) throws IOException {
        int width = -1;
        int height = -1;
        long[] sumA = null;
        long[] sumR = null;
        long[] sumG = null;
        long[] sumB = null;
        int[] count = null;
        int imageCount = 0;
        for (Path root : roots) {
            Path path = root.resolve(tileKey);
            if (!Files.isRegularFile(path)) {
                continue;
            }
            BufferedImage image = ImageIO.read(path.toFile());
            if (image == null) {
                continue;
            }
            if (width < 0) {
                width = image.getWidth();
                height = image.getHeight();
                int pixels = width * height;
                sumA = new long[pixels];
                sumR = new long[pixels];
                sumG = new long[pixels];
                sumB = new long[pixels];
                count = new int[pixels];
            }
            if (image.getWidth() != width || image.getHeight() != height) {
                continue;
            }
            imageCount++;
            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    int argb = image.getRGB(x, y);
                    int alpha = (argb >>> 24) & 0xff;
                    if (alpha < 10) {
                        continue;
                    }
                    int offset = y * width + x;
                    sumA[offset] += alpha;
                    sumR[offset] += (argb >>> 16) & 0xff;
                    sumG[offset] += (argb >>> 8) & 0xff;
                    sumB[offset] += argb & 0xff;
                    count[offset]++;
                }
            }
        }
        if (imageCount == 0 || width < 0) {
            return null;
        }
        BufferedImage out = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int offset = y * width + x;
                int n = count[offset];
                if (n == 0) {
                    out.setRGB(x, y, 0);
                    continue;
                }
                int a = (int) Math.round(sumA[offset] / (double) n);
                int r = (int) Math.round(sumR[offset] / (double) n);
                int g = (int) Math.round(sumG[offset] / (double) n);
                int b = (int) Math.round(sumB[offset] / (double) n);
                out.setRGB(x, y, (a << 24) | (r << 16) | (g << 8) | b);
            }
        }
        return out;
    }

    private static void writeJsonAtomic(Path targetFile, Object payload) throws IOException {
        Files.createDirectories(targetFile.getParent());
        Path tempFile = Files.createTempFile(targetFile.getParent(), targetFile.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), payload);
        try {
            Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception ignored) {
            Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void clearDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            List<Path> items = paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path path : items) {
                if (!path.equals(dir)) {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static void clearDirectoryQuietly(Path dir) {
        try {
            clearDirectory(dir);
            Files.deleteIfExists(dir);
        } catch (Exception ignored) {
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asList(Object value) {
        if (!(value instanceof List<?>)) {
            return List.of();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : (List<?>) value) {
            if (item instanceof Map<?, ?>) {
                result.add((Map<String, Object>) item);
            }
        }
        return result;
    }

    private static String stringValue(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    private static String stripJson(String name) {
        return name.endsWith(".json") ? name.substring(0, name.length() - 5) : name;
    }

    private static final class OriginResult {
        private final int snapshotCount;

        private OriginResult(int snapshotCount) {
            this.snapshotCount = snapshotCount;
        }
    }

    private static final class AggregateSnapshot {
        private final String type;
        private final String periodKey;
        private final int year;
        private final String month;
        private final int weekdayIso;
        private final String departureTime;
        private final int sampleCount;
        private final int tileCount;
        private final String totalNormalizedTileOverlayTemplate;

        private AggregateSnapshot(
                String type,
                String periodKey,
                int year,
                String month,
                int weekdayIso,
                String departureTime,
                int sampleCount,
                int tileCount,
                String totalNormalizedTileOverlayTemplate
        ) {
            this.type = type;
            this.periodKey = periodKey;
            this.year = year;
            this.month = month;
            this.weekdayIso = weekdayIso;
            this.departureTime = departureTime;
            this.sampleCount = sampleCount;
            this.tileCount = tileCount;
            this.totalNormalizedTileOverlayTemplate = totalNormalizedTileOverlayTemplate;
        }

        private Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("id", type + "-" + periodKey + "-w" + weekdayIso + "-" + departureTime.replace(":", ""));
            payload.put("aggregate", true);
            payload.put("type", type);
            payload.put("periodKey", periodKey);
            payload.put("year", year);
            payload.put("month", month);
            payload.put("weekdayIso", weekdayIso);
            payload.put("departureTime", departureTime);
            payload.put("sampleCount", sampleCount);
            payload.put("tileCount", tileCount);
            payload.put("availableModes", List.of("totalNormalized"));
            payload.put("totalNormalizedColorMinMinutes", 0);
            payload.put("totalNormalizedColorMaxMinutes", 800);
            payload.put("totalNormalizedTileOverlayTemplate", totalNormalizedTileOverlayTemplate);
            payload.put("overlayTileMaxZoom", 12);
            payload.put("contourPolygons", List.of());
            return payload;
        }
    }
}
