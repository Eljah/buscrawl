import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class BusRawParquetCleanupJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Instant startedAt = Instant.now();
        Path rawDir = Path.of(System.getenv().getOrDefault(
                "BUS_PARQUET_DIR",
                "./var/bus/bus-data-parquet"
        ));
        Path compactedStateFile = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_STATE_FILE",
                rawDir.getParent().resolve("bus-data-parquet-compacted").resolve("compaction-state.json").toString()
        ));
        Path manifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_FILE",
                rawDir.getParent().resolve("bus-data-parquet-manifest.tsv").toString()
        ));
        Path legacyManifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE",
                rawDir.getParent().resolve("bus-data-parquet-legacy-manifest.tsv").toString()
        ));
        Path cleanupStateFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CLEANUP_STATE_FILE",
                rawDir.getParent().resolve("bus-data-parquet-cleanup-state.json").toString()
        ));
        int maxFiles = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CLEANUP_MAX_FILES",
                "1000"
        ));
        int maxRecords = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CLEANUP_MAX_RECORDS",
                "2000"
        ));
        Duration minAge = Duration.ofHours(Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CLEANUP_MIN_AGE_HOURS",
                "1"
        )));
        boolean dryRun = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CLEANUP_DRY_RUN",
                "true"
        ));

        if (!Files.isDirectory(rawDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + rawDir);
        }
        if (!Files.isRegularFile(compactedStateFile)) {
            throw new IllegalStateException("Compaction state not found: " + compactedStateFile);
        }

        CompactionState compactionState = MAPPER.readValue(compactedStateFile.toFile(), CompactionState.class);
        CleanupState cleanupState = readCleanupState(cleanupStateFile);
        Path normalizedRawDir = rawDir.toAbsolutePath().normalize();
        Instant cutoff = Instant.now().minus(minAge);

        System.out.printf(
                "BusRawParquetCleanupJob: start dryRun=%s rawDir=%s manifestOffset=%d cleanupOffset=%d "
                        + "legacyManifestOffset=%d legacyCleanupOffset=%d maxFiles=%d maxRecords=%d minAge=%s%n",
                dryRun,
                rawDir,
                compactionState.manifestOffset,
                cleanupState.manifestOffset,
                compactionState.legacyManifestOffset,
                cleanupState.legacyManifestOffset,
                maxFiles,
                maxRecords,
                minAge
        );

        CleanupResult result = new CleanupResult();
        if (Files.isRegularFile(legacyManifestFile)
                && cleanupState.legacyManifestOffset < compactionState.legacyManifestOffset) {
            cleanupManifest(
                    legacyManifestFile,
                    Math.max(0L, cleanupState.legacyManifestOffset),
                    Math.max(0L, compactionState.legacyManifestOffset),
                    normalizedRawDir,
                    cutoff,
                    maxFiles,
                    maxRecords,
                    dryRun,
                    result
            );
            cleanupState.legacyManifestOffset = result.nextOffset;
            cleanupState.updatedAt = Instant.now().toString();
            writeCleanupState(cleanupStateFile, cleanupState);
            if (result.stopped()) {
                printComplete(result, startedAt);
                return;
            }
        }

        result.nextOffset = cleanupState.manifestOffset;
        if (Files.isRegularFile(manifestFile)
                && cleanupState.manifestOffset < compactionState.manifestOffset) {
            cleanupManifest(
                    manifestFile,
                    Math.max(0L, cleanupState.manifestOffset),
                    Math.max(0L, compactionState.manifestOffset),
                    normalizedRawDir,
                    cutoff,
                    maxFiles,
                    maxRecords,
                    dryRun,
                    result
            );
            cleanupState.manifestOffset = result.nextOffset;
            cleanupState.updatedAt = Instant.now().toString();
            writeCleanupState(cleanupStateFile, cleanupState);
        }

        cleanupState.updatedAt = Instant.now().toString();
        writeCleanupState(cleanupStateFile, cleanupState);
        printComplete(result, startedAt);
    }

    private static void cleanupManifest(
            Path manifestFile,
            long startOffset,
            long confirmedEndOffset,
            Path normalizedRawDir,
            Instant cutoff,
            int maxFiles,
            int maxRecords,
            boolean dryRun,
            CleanupResult result
    ) throws Exception {
        long safeEndOffset;
        try (RandomAccessFile reader = new RandomAccessFile(manifestFile.toFile(), "r")) {
            safeEndOffset = Math.min(Math.max(0L, confirmedEndOffset), reader.length());
            reader.seek(Math.min(Math.max(0L, startOffset), safeEndOffset));
            result.nextOffset = reader.getFilePointer();
            String line;
            while (reader.getFilePointer() < safeEndOffset && (line = reader.readLine()) != null) {
                long lineOffset = reader.getFilePointer();
                if (lineOffset > safeEndOffset) {
                    break;
                }
                if (result.scanned >= maxRecords) {
                    result.stopReason = "maxRecords";
                    return;
                }
                result.scanned++;
                IncrementalParquetSupport.ParquetFileInfo info = parseManifestLine(line);
                if (info == null) {
                    result.nextOffset = reader.getFilePointer();
                    continue;
                }
                Path path = Path.of(info.path).toAbsolutePath().normalize();
                if (!path.startsWith(normalizedRawDir)) {
                    result.skippedOutsideRawDir++;
                    result.nextOffset = reader.getFilePointer();
                    continue;
                }
                if (!Files.exists(path)) {
                    result.missing++;
                    result.nextOffset = reader.getFilePointer();
                    continue;
                }
                if (!Files.isRegularFile(path)) {
                    result.skippedNonRegular++;
                    result.nextOffset = reader.getFilePointer();
                    continue;
                }
                Instant modifiedAt = Files.getLastModifiedTime(path).toInstant();
                if (modifiedAt.isAfter(cutoff)) {
                    result.stopReason = "minAge";
                    return;
                }
                long size = Files.size(path);
                result.candidates++;
                result.bytes += size;
                if (!dryRun) {
                    Files.deleteIfExists(path);
                    result.deleted++;
                }
                result.nextOffset = reader.getFilePointer();
                if (result.deleted >= maxFiles || (dryRun && result.candidates >= maxFiles)) {
                    result.stopReason = "maxFiles";
                    return;
                }
            }
            result.nextOffset = reader.getFilePointer();
        }
    }

    private static IncrementalParquetSupport.ParquetFileInfo parseManifestLine(String line) {
        int separator = line.indexOf('\t');
        if (separator <= 0 || separator >= line.length() - 1) {
            return null;
        }
        String modifiedAt = line.substring(0, separator);
        if (IncrementalParquetSupport.parseInstant(modifiedAt) == null) {
            return null;
        }
        IncrementalParquetSupport.ParquetFileInfo info = new IncrementalParquetSupport.ParquetFileInfo();
        info.modifiedAt = modifiedAt;
        info.path = line.substring(separator + 1);
        return info;
    }

    private static CleanupState readCleanupState(Path stateFile) throws IOException {
        if (!Files.exists(stateFile)) {
            return new CleanupState();
        }
        return MAPPER.readValue(stateFile.toFile(), CleanupState.class);
    }

    private static void writeCleanupState(Path stateFile, CleanupState state) throws IOException {
        Files.createDirectories(stateFile.getParent());
        Path temp = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), state);
        Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void printComplete(CleanupResult result, Instant startedAt) {
        System.out.printf(
                "BusRawParquetCleanupJob: complete scanned=%d candidates=%d deleted=%d missing=%d "
                        + "skippedOutsideRawDir=%d skippedNonRegular=%d bytes=%d nextOffset=%d stopReason=%s elapsedMs=%d%n",
                result.scanned,
                result.candidates,
                result.deleted,
                result.missing,
                result.skippedOutsideRawDir,
                result.skippedNonRegular,
                result.bytes,
                result.nextOffset,
                result.stopReason,
                Duration.between(startedAt, Instant.now()).toMillis()
        );
    }

    public static final class CompactionState {
        public String updatedAt;
        public String lastProcessedModifiedAt;
        public String lastProcessedPath;
        public long manifestOffset;
        public long legacyManifestOffset;
        public boolean legacyManifestComplete;
    }

    public static final class CleanupState {
        public String updatedAt;
        public long manifestOffset;
        public long legacyManifestOffset;
    }

    private static final class CleanupResult {
        private int scanned;
        private int candidates;
        private int deleted;
        private int missing;
        private int skippedOutsideRawDir;
        private int skippedNonRegular;
        private long bytes;
        private long nextOffset;
        private String stopReason = "end";

        private boolean stopped() {
            return !"end".equals(stopReason);
        }
    }
}
