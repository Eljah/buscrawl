import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class BusRawParquetDatePartitionMigrationJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Instant startedAt = Instant.now();
        Path rawDir = Path.of(System.getenv().getOrDefault(
                "BUS_PARQUET_DIR",
                "./var/bus/bus-data-parquet"
        )).toAbsolutePath().normalize();
        Path sourceManifest = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_LEGACY_MANIFEST_FILE",
                rawDir.getParent().resolve("bus-data-parquet-legacy-manifest.tsv").toString()
        ));
        Path buildingManifest = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MIGRATED_LEGACY_MANIFEST_BUILDING_FILE",
                sourceManifest.resolveSibling(sourceManifest.getFileName() + ".migrated-building").toString()
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_STATE_FILE",
                sourceManifest.resolveSibling(sourceManifest.getFileName() + ".date-migration-state.json").toString()
        ));
        ZoneId dateZone = ZoneId.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_ZONE",
                "Europe/Moscow"
        ));
        int maxRecordsPerRun = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_MAX_RECORDS_PER_RUN",
                "5000"
        ));
        int checkpointEvery = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_CHECKPOINT_EVERY",
                "200"
        ));
        long sleepEveryRecords = Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_EVERY_RECORDS",
                "200"
        ));
        long sleepMillis = Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_SLEEP_MILLIS",
                "50"
        ));
        boolean finalizeMigration = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_DATE_MIGRATION_FINALIZE",
                "false"
        ));

        if (!Files.isDirectory(rawDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + rawDir);
        }
        if (!Files.isRegularFile(sourceManifest)) {
            throw new IllegalStateException("Legacy manifest not found: " + sourceManifest);
        }

        MigrationState state = readState(stateFile);
        state.sourceManifest = sourceManifest.toAbsolutePath().normalize().toString();
        state.buildingManifest = buildingManifest.toAbsolutePath().normalize().toString();
        state.rawDir = rawDir.toString();
        state.updatedAt = startedAt.toString();
        writeState(stateFile, state);

        if (finalizeMigration) {
            finalizeMigration(sourceManifest, buildingManifest, stateFile, state);
            printSummary(startedAt, state, 0, 0, 0, 0, true);
            return;
        }

        Set<String> alreadyWritten = loadManifestPaths(buildingManifest);
        long scanned = 0L;
        long moved = 0L;
        long alreadyPartitioned = 0L;
        long missing = 0L;

        Files.createDirectories(buildingManifest.getParent());
        try (RandomAccessFile reader = new RandomAccessFile(sourceManifest.toFile(), "r");
             FileChannel channel = FileChannel.open(
                     buildingManifest,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.APPEND
             );
             BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8))) {
            long safeOffset = Math.min(Math.max(0L, state.offset), reader.length());
            reader.seek(safeOffset);
            String line;
            while ((line = reader.readLine()) != null) {
                long nextOffset = reader.getFilePointer();
                if (maxRecordsPerRun > 0 && scanned >= maxRecordsPerRun) {
                    flushAndCheckpoint(writer, channel, stateFile, state, false);
                    printSummary(startedAt, state, scanned, moved, alreadyPartitioned, missing, false);
                    System.exit(75);
                }
                scanned++;
                if (sleepEveryRecords > 0 && scanned % sleepEveryRecords == 0 && sleepMillis > 0) {
                    Thread.sleep(sleepMillis);
                }

                IncrementalParquetSupport.ParquetFileInfo info = parseManifestLine(line);
                if (info == null) {
                    state.offset = nextOffset;
                    continue;
                }
                Path original = Path.of(info.path).toAbsolutePath().normalize();
                Path migrated = migratedPath(rawDir, original, info.modifiedAt, dateZone);
                Path pathToWrite = null;

                if (Files.isRegularFile(migrated)) {
                    pathToWrite = migrated;
                    if (Files.isRegularFile(original) && !original.equals(migrated)) {
                        tryDeleteDuplicateOriginal(original, migrated);
                    }
                    alreadyPartitioned++;
                } else if (Files.isRegularFile(original)) {
                    Files.createDirectories(migrated.getParent());
                    Files.move(original, migrated, StandardCopyOption.ATOMIC_MOVE);
                    pathToWrite = migrated;
                    moved++;
                } else {
                    Path fallback = findAlreadyMigratedFallback(rawDir, original, info.modifiedAt, dateZone);
                    if (fallback != null) {
                        pathToWrite = fallback;
                        alreadyPartitioned++;
                    } else {
                        missing++;
                    }
                }

                if (pathToWrite != null) {
                    String normalizedPath = pathToWrite.toAbsolutePath().normalize().toString();
                    if (alreadyWritten.add(normalizedPath)) {
                        writer.write(info.modifiedAt);
                        writer.write('\t');
                        writer.write(normalizedPath);
                        writer.write('\n');
                        state.written++;
                    }
                }

                state.offset = nextOffset;
                state.scanned += 1;
                state.moved += pathToWrite != null && Files.isRegularFile(pathToWrite) && !original.equals(pathToWrite) ? 1 : 0;
                state.missing += pathToWrite == null ? 1 : 0;
                if (scanned % checkpointEvery == 0) {
                    flushAndCheckpoint(writer, channel, stateFile, state, false);
                }
            }
            flushAndCheckpoint(writer, channel, stateFile, state, true);
        }

        printSummary(startedAt, state, scanned, moved, alreadyPartitioned, missing, true);
    }

    private static void finalizeMigration(
            Path sourceManifest,
            Path buildingManifest,
            Path stateFile,
            MigrationState state
    ) throws IOException {
        if (!Files.isRegularFile(buildingManifest)) {
            throw new IllegalStateException("Migrated building manifest not found: " + buildingManifest);
        }
        Path backup = sourceManifest.resolveSibling(sourceManifest.getFileName() + ".before-date-migration");
        Path sorted = sourceManifest.resolveSibling(sourceManifest.getFileName() + ".migrated-sorted");
        try (var lines = Files.lines(buildingManifest, StandardCharsets.UTF_8)) {
            Files.write(
                    sorted,
                    lines.filter(line -> !line.isBlank()).distinct().sorted().collect(Collectors.toList()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }
        if (!Files.exists(backup)) {
            Files.copy(sourceManifest, backup);
        }
        Files.move(sorted, sourceManifest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        state.finalized = true;
        state.finalizedAt = Instant.now().toString();
        state.updatedAt = state.finalizedAt;
        writeState(stateFile, state);
    }

    private static Path migratedPath(Path rawDir, Path original, String modifiedAtText, ZoneId zone) {
        Instant modifiedAt = IncrementalParquetSupport.parseInstant(modifiedAtText);
        LocalDate rawDate = modifiedAt == null ? LocalDate.now(zone) : modifiedAt.atZone(zone).toLocalDate();
        String fileName = original.getFileName() == null ? "unknown.parquet" : original.getFileName().toString();
        Path target = rawDir.resolve("rawDate=" + rawDate).resolve(fileName).toAbsolutePath().normalize();
        if (target.equals(original) || !Files.exists(target)) {
            return target;
        }
        String suffix = Integer.toHexString(original.toString().hashCode());
        String migratedName = fileName.endsWith(".parquet")
                ? fileName.substring(0, fileName.length() - ".parquet".length()) + "-migrated-" + suffix + ".parquet"
                : fileName + "-migrated-" + suffix;
        return rawDir.resolve("rawDate=" + rawDate).resolve(migratedName).toAbsolutePath().normalize();
    }

    private static Path findAlreadyMigratedFallback(Path rawDir, Path original, String modifiedAtText, ZoneId zone) {
        Path target = migratedPath(rawDir, original, modifiedAtText, zone);
        if (Files.isRegularFile(target)) {
            return target;
        }
        return null;
    }

    private static void tryDeleteDuplicateOriginal(Path original, Path migrated) {
        try {
            if (Files.size(original) == Files.size(migrated)) {
                Files.deleteIfExists(original);
            }
        } catch (IOException ignored) {
            // Leaving a duplicate is safer than deleting when the comparison cannot be made.
        }
    }

    private static void flushAndCheckpoint(
            BufferedWriter writer,
            FileChannel channel,
            Path stateFile,
            MigrationState state,
            boolean completed
    ) throws IOException {
        writer.flush();
        channel.force(false);
        state.completed = completed;
        state.updatedAt = Instant.now().toString();
        writeState(stateFile, state);
    }

    private static Set<String> loadManifestPaths(Path manifest) throws IOException {
        Set<String> paths = new HashSet<>();
        if (!Files.isRegularFile(manifest)) {
            return paths;
        }
        try (var lines = Files.lines(manifest, StandardCharsets.UTF_8)) {
            lines.forEach(line -> {
                int separator = line.indexOf('\t');
                if (separator > 0 && separator < line.length() - 1) {
                    paths.add(Path.of(line.substring(separator + 1)).toAbsolutePath().normalize().toString());
                }
            });
        }
        return paths;
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

    private static MigrationState readState(Path stateFile) throws IOException {
        if (!Files.isRegularFile(stateFile)) {
            return new MigrationState();
        }
        return MAPPER.readValue(stateFile.toFile(), MigrationState.class);
    }

    private static void writeState(Path stateFile, MigrationState state) throws IOException {
        Files.createDirectories(stateFile.getParent());
        Path temp = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), state);
        Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void printSummary(
            Instant startedAt,
            MigrationState state,
            long scanned,
            long moved,
            long alreadyPartitioned,
            long missing,
            boolean completed
    ) {
        System.out.printf(
                Locale.US,
                "BusRawParquetDatePartitionMigrationJob: completed=%s runScanned=%,d runMoved=%,d "
                        + "runAlreadyPartitioned=%,d runMissing=%,d totalScanned=%,d totalWritten=%,d "
                        + "offset=%,d elapsedMs=%,d%n",
                completed,
                scanned,
                moved,
                alreadyPartitioned,
                missing,
                state.scanned,
                state.written,
                state.offset,
                java.time.Duration.between(startedAt, Instant.now()).toMillis()
        );
    }

    public static final class MigrationState {
        public String updatedAt;
        public String sourceManifest;
        public String buildingManifest;
        public String rawDir;
        public long offset;
        public long scanned;
        public long moved;
        public long missing;
        public long written;
        public boolean completed;
        public boolean finalized;
        public String finalizedAt;
    }
}
