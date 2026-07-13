import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
        Instant startedAt = Instant.now();
        Path storageRoot = Paths.get(System.getenv().getOrDefault("BUS_STORAGE_ROOT", "./var/bus"));
        Path readyDir = Paths.get(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_READY_DIR",
                storageRoot.resolve("raw-json-spool").resolve("ready").toString()
        ));
        Path checkpointDir = Paths.get(System.getenv().getOrDefault(
                "BUS_STREAM_CHECKPOINT_DIR",
                storageRoot.resolve("bus-data-checkpoint").toString()
        ));
        Path stateFile = Paths.get(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_STATE_FILE",
                storageRoot.resolve("raw-spool-cleanup-state.json").toString()
        ));
        Duration minAge = Duration.ofHours(Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_MIN_AGE_HOURS",
                "6"
        )));
        int maxFiles = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_MAX_FILES",
                "1000"
        ));
        int maxRecords = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_SPOOL_CLEANUP_MAX_RECORDS",
                "20000"
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

        System.out.printf(
                "Raw spool cleanup start: dryRun=%s minAge=%s maxFiles=%d maxRecords=%d state=%s%n",
                dryRun,
                minAge,
                maxFiles,
                maxRecords,
                stateFile
        );

        CleanupState state = readState(stateFile);
        Set<Long> committedBatches = readCommittedBatches(commitDir);
        System.out.printf(
                "Raw spool cleanup phase committed-batches: count=%d lastCompletedBatch=%d currentSourceLog=%s offset=%d elapsedMs=%d%n",
                committedBatches.size(),
                state.lastCompletedBatch,
                state.currentSourceLog,
                state.currentSourceOffset,
                elapsedMs(startedAt)
        );

        List<SourceLogRef> sourceLogs = listSourceLogs(sourceLogDir, committedBatches, state);
        System.out.printf(
                "Raw spool cleanup phase source-logs: selected=%d first=%s elapsedMs=%d%n",
                sourceLogs.size(),
                sourceLogs.isEmpty() ? "-" : sourceLogs.get(0).path.getFileName(),
                elapsedMs(startedAt)
        );
        if (sourceLogs.isEmpty()) {
            state.updatedAt = Instant.now().toString();
            writeState(stateFile, state);
            System.out.printf(
                    "Raw spool cleanup complete: dryRun=%s committedBatches=%d sourceLogs=0 scanned=0 candidates=0 deleted=0 bytes=0 lastCompletedBatch=%d elapsedMs=%d%n",
                    dryRun,
                    committedBatches.size(),
                    state.lastCompletedBatch,
                    elapsedMs(startedAt)
            );
            return;
        }

        CleanupResult result = processSourceLogs(sourceLogs, readyDir, minAge, maxFiles, maxRecords, dryRun, state);
        state.updatedAt = Instant.now().toString();
        writeState(stateFile, state);

        System.out.printf(
                "Raw spool cleanup complete: dryRun=%s committedBatches=%d sourceLogs=%d scanned=%d "
                        + "candidates=%d deleted=%d bytes=%d stoppedBy=%s lastCompletedBatch=%d currentSourceLog=%s "
                        + "offset=%d elapsedMs=%d%n",
                dryRun,
                committedBatches.size(),
                sourceLogs.size(),
                result.scannedRecords,
                result.candidates,
                result.deleted,
                result.bytes,
                result.stoppedBy,
                state.lastCompletedBatch,
                state.currentSourceLog,
                state.currentSourceOffset,
                elapsedMs(startedAt)
        );
    }

    private static CleanupResult processSourceLogs(
            List<SourceLogRef> sourceLogs,
            Path readyDir,
            Duration minAge,
            int maxFiles,
            int maxRecords,
            boolean dryRun,
            CleanupState state
    ) throws Exception {
        CleanupResult result = new CleanupResult();
        Instant cutoff = Instant.now().minus(minAge);
        Path normalizedReadyDir = readyDir.toAbsolutePath().normalize();
        for (SourceLogRef sourceLog : sourceLogs) {
            long offset = sourceLog.path.getFileName().toString().equals(state.currentSourceLog)
                    ? Math.max(0L, state.currentSourceOffset)
                    : 0L;
            System.out.printf(
                    "Raw spool cleanup phase source-log: batch=%d file=%s offset=%d%n",
                    sourceLog.batchId,
                    sourceLog.path.getFileName(),
                    offset
            );
            try (RandomAccessFile reader = new RandomAccessFile(sourceLog.path.toFile(), "r")) {
                reader.seek(Math.min(offset, reader.length()));
                while (reader.getFilePointer() < reader.length()) {
                    long lineOffset = reader.getFilePointer();
                    String line = reader.readLine();
                    long nextOffset = reader.getFilePointer();
                    if (line == null) {
                        break;
                    }
                    if (result.scannedRecords >= maxRecords) {
                        result.stoppedBy = "maxRecords";
                        state.currentSourceLog = sourceLog.path.getFileName().toString();
                        state.currentSourceOffset = lineOffset;
                        return result;
                    }
                    result.scannedRecords++;
                    Path readyFile = readyFileFromSourceLine(line, normalizedReadyDir);
                    if (readyFile == null) {
                        state.currentSourceLog = sourceLog.path.getFileName().toString();
                        state.currentSourceOffset = nextOffset;
                        continue;
                    }
                    FileDecision decision = decideFile(readyFile, cutoff);
                    if (decision == FileDecision.NOT_OLD_ENOUGH) {
                        result.stoppedBy = "minAge";
                        state.currentSourceLog = sourceLog.path.getFileName().toString();
                        state.currentSourceOffset = lineOffset;
                        return result;
                    }
                    if (decision == FileDecision.DELETE) {
                        result.candidates++;
                        long size = Files.size(readyFile);
                        result.bytes += size;
                        if (!dryRun) {
                            Files.deleteIfExists(readyFile);
                            result.deleted++;
                        }
                        if (result.deleted >= maxFiles || (dryRun && result.candidates >= maxFiles)) {
                            result.stoppedBy = "maxFiles";
                            state.currentSourceLog = sourceLog.path.getFileName().toString();
                            state.currentSourceOffset = nextOffset;
                            return result;
                        }
                    }
                    state.currentSourceLog = sourceLog.path.getFileName().toString();
                    state.currentSourceOffset = nextOffset;
                }
            }
            state.lastCompletedBatch = Math.max(state.lastCompletedBatch, sourceLog.batchId);
            state.currentSourceLog = null;
            state.currentSourceOffset = 0L;
        }
        result.stoppedBy = "end";
        return result;
    }

    private static FileDecision decideFile(Path readyFile, Instant cutoff) throws IOException {
        if (!Files.isRegularFile(readyFile)) {
            return FileDecision.SKIP;
        }
        BasicFileAttributes attrs = Files.readAttributes(readyFile, BasicFileAttributes.class);
        if (attrs.lastModifiedTime().toInstant().isAfter(cutoff)) {
            return FileDecision.NOT_OLD_ENOUGH;
        }
        return FileDecision.DELETE;
    }

    private static Path readyFileFromSourceLine(String line, Path normalizedReadyDir) throws IOException {
        if (line.isBlank() || line.equals("v1")) {
            return null;
        }
        JsonNode node = OBJECT_MAPPER.readTree(new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));
        JsonNode pathNode = node.get("path");
        if (pathNode == null || pathNode.asText().isBlank()) {
            return null;
        }
        Path sourcePath = pathFromSparkUri(pathNode.asText()).toAbsolutePath().normalize();
        if (sourcePath.getParent() == null || !sourcePath.getParent().equals(normalizedReadyDir)) {
            return null;
        }
        return sourcePath;
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

    private static List<SourceLogRef> listSourceLogs(
            Path sourceLogDir,
            Set<Long> committedBatches,
            CleanupState state
    ) throws IOException {
        List<SourceLogRef> regularLogs = new ArrayList<>();
        SourceLogRef latestCommittedCompact = null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceLogDir)) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (name.startsWith(".") || name.endsWith(".crc")) {
                    continue;
                }
                Long batchId = parseBatchId(name);
                if (batchId == null || !committedBatches.contains(batchId)) {
                    continue;
                }
                SourceLogRef ref = new SourceLogRef(batchId, path);
                if (name.endsWith(".compact")) {
                    if (latestCommittedCompact == null || batchId > latestCommittedCompact.batchId) {
                        latestCommittedCompact = ref;
                    }
                } else {
                    regularLogs.add(ref);
                }
            }
        }

        long floorBatch = Math.max(0L, state.lastCompletedBatch);
        List<SourceLogRef> result = new ArrayList<>();
        if (latestCommittedCompact != null && latestCommittedCompact.batchId > floorBatch) {
            result.add(latestCommittedCompact);
            floorBatch = latestCommittedCompact.batchId;
        } else if (state.currentSourceLog != null && latestCommittedCompact != null
                && latestCommittedCompact.path.getFileName().toString().equals(state.currentSourceLog)) {
            result.add(latestCommittedCompact);
            floorBatch = latestCommittedCompact.batchId;
        }
        long regularLogFloorBatch = floorBatch;
        regularLogs.stream()
                .filter(ref -> ref.batchId > regularLogFloorBatch
                        || (state.currentSourceLog != null && ref.path.getFileName().toString().equals(state.currentSourceLog)))
                .sorted(Comparator.comparingLong(ref -> ref.batchId))
                .forEach(result::add);
        return result;
    }

    private static Long parseBatchId(String name) {
        if (name.endsWith(".compact")) {
            name = name.substring(0, name.length() - ".compact".length());
        }
        try {
            return Long.parseLong(name);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Path pathFromSparkUri(String value) {
        if (value.startsWith("file:")) {
            return Paths.get(URI.create(value));
        }
        return Paths.get(value);
    }

    private static CleanupState readState(Path stateFile) throws IOException {
        if (!Files.exists(stateFile)) {
            return new CleanupState();
        }
        return OBJECT_MAPPER.readValue(stateFile.toFile(), CleanupState.class);
    }

    private static void writeState(Path stateFile, CleanupState state) throws IOException {
        Files.createDirectories(stateFile.getParent());
        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static long elapsedMs(Instant startedAt) {
        return Duration.between(startedAt, Instant.now()).toMillis();
    }

    private enum FileDecision {
        DELETE,
        NOT_OLD_ENOUGH,
        SKIP
    }

    public static final class CleanupState {
        public String updatedAt;
        public long lastCompletedBatch;
        public String currentSourceLog;
        public long currentSourceOffset;
    }

    private static final class SourceLogRef {
        private final long batchId;
        private final Path path;

        private SourceLogRef(long batchId, Path path) {
            this.batchId = batchId;
            this.path = path;
        }
    }

    private static final class CleanupResult {
        private int scannedRecords;
        private int candidates;
        private int deleted;
        private long bytes;
        private String stoppedBy = "end";
    }
}
