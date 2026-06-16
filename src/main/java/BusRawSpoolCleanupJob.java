import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BusRawSpoolCleanupJob {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Path storageRoot = Paths.get(System.getenv().getOrDefault("BUS_STORAGE_ROOT", "./var/bus"));
        Path readyDir = Paths.get(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_READY_DIR",
                storageRoot.resolve("raw-json-spool").resolve("ready").toString()
        ));
        Path checkpointDir = Paths.get(System.getenv().getOrDefault(
                "BUS_STREAM_CHECKPOINT_DIR",
                storageRoot.resolve("bus-data-checkpoint").toString()
        ));
        Duration minAge = Duration.ofHours(Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS",
                "6"
        )));
        int maxFiles = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_MAX_FILES",
                "5000"
        ));
        boolean dryRun = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_DRY_RUN",
                "true"
        ));

        Path sourceLogDir = checkpointDir.resolve("sources").resolve("0");
        Path commitDir = checkpointDir.resolve("commits");
        if (!Files.isDirectory(readyDir) || !Files.isDirectory(sourceLogDir) || !Files.isDirectory(commitDir)) {
            throw new IllegalStateException("Missing ready/source/commit directory: ready=" + readyDir
                    + ", sourceLog=" + sourceLogDir + ", commits=" + commitDir);
        }

        Set<Long> committedBatches = readCommittedBatches(commitDir);
        List<Path> sourceLogs = listSourceLogs(sourceLogDir, committedBatches);
        Set<Path> committedReadyFiles = readCommittedReadyFiles(sourceLogs, readyDir);
        List<Path> deleteCandidates = selectDeleteCandidates(committedReadyFiles, minAge, maxFiles);

        long bytes = 0L;
        int deleted = 0;
        for (Path path : deleteCandidates) {
            long size = Files.exists(path) ? Files.size(path) : 0L;
            bytes += size;
            if (!dryRun) {
                Files.deleteIfExists(path);
                deleted++;
            }
        }

        System.out.printf(
                "Raw spool cleanup complete: dryRun=%s, committedBatches=%d, sourceLogs=%d, "
                        + "committedReadyFiles=%d, candidates=%d, deleted=%d, bytes=%d%n",
                dryRun,
                committedBatches.size(),
                sourceLogs.size(),
                committedReadyFiles.size(),
                deleteCandidates.size(),
                deleted,
                bytes
        );
    }

    private static Set<Long> readCommittedBatches(Path commitDir) throws IOException {
        Set<Long> batches = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(commitDir)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (name.startsWith(".") || name.endsWith(".crc")) {
                    continue;
                }
                try {
                    batches.add(Long.parseLong(name));
                } catch (NumberFormatException ignored) {
                    // Spark can add metadata files; only numeric commit IDs are batch IDs.
                }
            }
        }
        return batches;
    }

    private static List<Path> listSourceLogs(Path sourceLogDir, Set<Long> committedBatches) throws IOException {
        List<Path> sourceLogs = new ArrayList<>();
        long latestCommittedCompactBatch = -1L;
        Path latestCommittedCompact = null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceLogDir)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (name.startsWith(".") || name.endsWith(".crc")) {
                    continue;
                }
                if (name.endsWith(".compact")) {
                    String batchName = name.substring(0, name.length() - ".compact".length());
                    try {
                        long batchId = Long.parseLong(batchName);
                        if (committedBatches.contains(batchId) && batchId > latestCommittedCompactBatch) {
                            latestCommittedCompactBatch = batchId;
                            latestCommittedCompact = path;
                        }
                    } catch (NumberFormatException ignored) {
                        // Only numeric compact source log files are valid.
                    }
                    continue;
                }
                try {
                    long batchId = Long.parseLong(name);
                    if (committedBatches.contains(batchId) && batchId > latestCommittedCompactBatch) {
                        sourceLogs.add(path);
                    }
                } catch (NumberFormatException ignored) {
                    // Only numeric source log files are per-batch file lists.
                }
            }
        }
        long compactBatch = latestCommittedCompactBatch;
        sourceLogs.removeIf(path -> batchIdFromPath(path) <= compactBatch);
        sourceLogs.sort(Comparator.comparingLong(BusRawSpoolCleanupJob::batchIdFromPath));
        if (latestCommittedCompact != null) {
            sourceLogs.add(0, latestCommittedCompact);
        }
        return sourceLogs;
    }

    private static Set<Path> readCommittedReadyFiles(List<Path> sourceLogs, Path readyDir) throws IOException {
        Path normalizedReadyDir = readyDir.toAbsolutePath().normalize();
        Set<Path> paths = new HashSet<>();
        for (Path sourceLog : sourceLogs) {
            try (BufferedReader reader = Files.newBufferedReader(sourceLog, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank() || line.equals("v1")) {
                        continue;
                    }
                    JsonNode node = OBJECT_MAPPER.readTree(line);
                    JsonNode pathNode = node.get("path");
                    if (pathNode == null || pathNode.asText().isBlank()) {
                        continue;
                    }
                    Path sourcePath = pathFromSparkUri(pathNode.asText()).toAbsolutePath().normalize();
                    if (sourcePath.getParent() != null && sourcePath.getParent().equals(normalizedReadyDir)) {
                        paths.add(sourcePath);
                    }
                }
            }
        }
        return paths;
    }

    private static List<Path> selectDeleteCandidates(Set<Path> committedReadyFiles, Duration minAge, int maxFiles)
            throws IOException {
        Instant cutoff = Instant.now().minus(minAge);
        List<Path> candidates = new ArrayList<>();
        for (Path path : committedReadyFiles) {
            if (!Files.isRegularFile(path)) {
                continue;
            }
            BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
            if (attrs.lastModifiedTime().toInstant().isAfter(cutoff)) {
                continue;
            }
            candidates.add(path);
        }
        candidates.sort(Comparator.comparing(BusRawSpoolCleanupJob::lastModifiedOrEpoch));
        if (candidates.size() > maxFiles) {
            return new ArrayList<>(candidates.subList(0, maxFiles));
        }
        return candidates;
    }

    private static Path pathFromSparkUri(String value) {
        if (value.startsWith("file:")) {
            return Paths.get(URI.create(value));
        }
        return Paths.get(value);
    }

    private static long batchIdFromPath(Path path) {
        String name = path.getFileName().toString();
        if (name.endsWith(".compact")) {
            name = name.substring(0, name.length() - ".compact".length());
        }
        return Long.parseLong(name);
    }

    private static Instant lastModifiedOrEpoch(Path path) {
        try {
            return Files.getLastModifiedTime(path).toInstant();
        } catch (IOException e) {
            return Instant.EPOCH;
        }
    }
}
