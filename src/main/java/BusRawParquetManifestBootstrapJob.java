import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class BusRawParquetManifestBootstrapJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Instant startedAt = Instant.now();
        Path inputDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path finalManifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_FILE",
                inputDir.getParent().resolve("bus-data-parquet-legacy-manifest.tsv").toString()
        ));
        Path buildingManifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BUILDING_FILE",
                finalManifestFile.resolveSibling(finalManifestFile.getFileName() + ".building").toString()
        ));
        Path currentManifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_CURRENT_MANIFEST_FILE",
                inputDir.getParent().resolve("bus-data-parquet-manifest.tsv").toString()
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_STATE_FILE",
                finalManifestFile.resolveSibling(finalManifestFile.getFileName() + ".bootstrap-state.json").toString()
        ));
        boolean finalizeManifest = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_FINALIZE",
                "false"
        ));
        boolean force = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_FORCE",
                "false"
        ));
        int maxAppendPerRun = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_MAX_APPEND_PER_RUN",
                "50000"
        ));
        int checkpointEvery = Integer.parseInt(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_CHECKPOINT_EVERY",
                "1000"
        ));
        long sleepEveryRecords = Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_SLEEP_EVERY_RECORDS",
                "2000"
        ));
        long sleepMillis = Long.parseLong(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_SLEEP_MILLIS",
                "25"
        ));

        if (!Files.isDirectory(inputDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + inputDir);
        }
        Files.createDirectories(finalManifestFile.getParent());

        if (Files.exists(finalManifestFile) && !force) {
            System.out.println("BusRawParquetManifestBootstrapJob: final manifest already exists: " + finalManifestFile);
            return;
        }
        if (force) {
            Files.deleteIfExists(buildingManifestFile);
            Files.deleteIfExists(stateFile);
        }

        Set<String> knownPaths = new HashSet<>();
        loadManifestPaths(currentManifestFile, knownPaths);
        loadManifestPaths(buildingManifestFile, knownPaths);

        BootstrapState state = readState(stateFile);
        state.startedAt = state.startedAt == null ? startedAt.toString() : state.startedAt;
        state.lastRunStartedAt = startedAt.toString();
        state.inputDir = inputDir.toAbsolutePath().normalize().toString();
        state.buildingManifestFile = buildingManifestFile.toAbsolutePath().normalize().toString();
        state.finalManifestFile = finalManifestFile.toAbsolutePath().normalize().toString();
        state.currentManifestFile = currentManifestFile.toAbsolutePath().normalize().toString();
        writeState(stateFile, state);

        long scanned = 0L;
        long appended = 0L;
        long skippedKnown = 0L;
        long skippedNonParquet = 0L;
        long skippedNonRegular = 0L;

        try (FileChannel channel = FileChannel.open(
                buildingManifestFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
             BufferedWriter writer = new BufferedWriter(Channels.newWriter(channel, StandardCharsets.UTF_8))) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputDir)) {
                for (Path path : stream) {
                    scanned++;
                    if (sleepEveryRecords > 0 && scanned % sleepEveryRecords == 0 && sleepMillis > 0) {
                        Thread.sleep(sleepMillis);
                    }
                    String fileName = path.getFileName() == null ? "" : path.getFileName().toString();
                    if (!fileName.endsWith(".parquet")) {
                        skippedNonParquet++;
                        continue;
                    }
                    if (!Files.isRegularFile(path)) {
                        skippedNonRegular++;
                        continue;
                    }
                    String normalizedPath = path.toAbsolutePath().normalize().toString();
                    if (knownPaths.contains(normalizedPath)) {
                        skippedKnown++;
                        continue;
                    }
                    Instant modifiedAt = Files.getLastModifiedTime(path).toInstant();
                    writer.write(modifiedAt.toString());
                    writer.write('\t');
                    writer.write(normalizedPath);
                    writer.write('\n');
                    knownPaths.add(normalizedPath);
                    appended++;
                    state.totalAppended++;
                    state.lastAppendedPath = normalizedPath;
                    state.lastAppendedModifiedAt = modifiedAt.toString();

                    if (appended % checkpointEvery == 0) {
                        writer.flush();
                        channel.force(false);
                        updateRunState(state, scanned, appended, skippedKnown, skippedNonParquet, skippedNonRegular, false);
                        writeState(stateFile, state);
                    }
                    if (maxAppendPerRun > 0 && appended >= maxAppendPerRun) {
                        writer.flush();
                        channel.force(false);
                        updateRunState(state, scanned, appended, skippedKnown, skippedNonParquet, skippedNonRegular, false);
                        state.stopReason = "maxAppendPerRun";
                        writeState(stateFile, state);
                        printSummary(startedAt, scanned, appended, skippedKnown, skippedNonParquet, skippedNonRegular, false);
                        System.exit(75);
                    }
                }
            }
            writer.flush();
            channel.force(false);
        }

        updateRunState(state, scanned, appended, skippedKnown, skippedNonParquet, skippedNonRegular, true);
        state.completedAt = Instant.now().toString();
        state.stopReason = "completed";
        writeState(stateFile, state);

        if (finalizeManifest) {
            Path sortedManifest = finalManifestFile.resolveSibling(finalManifestFile.getFileName() + ".sorted");
            Set<String> currentManifestPaths = new HashSet<>();
            loadManifestPaths(currentManifestFile, currentManifestPaths);
            sortManifest(buildingManifestFile, sortedManifest, currentManifestPaths);
            Files.move(sortedManifest, finalManifestFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            state.finalizedAt = Instant.now().toString();
            writeState(stateFile, state);
            System.out.printf(
                    Locale.US,
                    "BusRawParquetManifestBootstrapJob: finalized legacy manifest %s%n",
                    finalManifestFile
            );
        }

        printSummary(startedAt, scanned, appended, skippedKnown, skippedNonParquet, skippedNonRegular, true);
    }

    private static void sortManifest(Path source, Path target, Set<String> excludedPaths) throws IOException {
        List<String> lines = Files.lines(source, StandardCharsets.UTF_8)
                .filter(line -> !line.isBlank())
                .filter(line -> {
                    int separator = line.indexOf('\t');
                    return separator > 0
                            && separator < line.length() - 1
                            && !excludedPaths.contains(Path.of(line.substring(separator + 1)).toAbsolutePath().normalize().toString());
                })
                .sorted()
                .collect(Collectors.toList());
        Path temp = target.resolveSibling(target.getFileName() + ".tmp");
        Files.write(temp, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void updateRunState(
            BootstrapState state,
            long scanned,
            long appended,
            long skippedKnown,
            long skippedNonParquet,
            long skippedNonRegular,
            boolean completed
    ) {
        state.updatedAt = Instant.now().toString();
        state.lastRunScanned = scanned;
        state.lastRunAppended = appended;
        state.lastRunSkippedKnown = skippedKnown;
        state.lastRunSkippedNonParquet = skippedNonParquet;
        state.lastRunSkippedNonRegular = skippedNonRegular;
        state.completed = completed;
    }

    private static void loadManifestPaths(Path manifestFile, Set<String> target) throws IOException {
        if (!Files.isRegularFile(manifestFile)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(manifestFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                int separator = line.indexOf('\t');
                if (separator > 0 && separator < line.length() - 1) {
                    target.add(Path.of(line.substring(separator + 1)).toAbsolutePath().normalize().toString());
                }
            }
        }
    }

    private static BootstrapState readState(Path stateFile) throws IOException {
        if (!Files.isRegularFile(stateFile)) {
            return new BootstrapState();
        }
        return MAPPER.readValue(stateFile.toFile(), BootstrapState.class);
    }

    private static void writeState(Path stateFile, BootstrapState state) throws IOException {
        Files.createDirectories(stateFile.getParent());
        Path temp = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), state);
        Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void printSummary(
            Instant startedAt,
            long scanned,
            long appended,
            long skippedKnown,
            long skippedNonParquet,
            long skippedNonRegular,
            boolean completed
    ) {
        System.out.printf(
                Locale.US,
                "BusRawParquetManifestBootstrapJob: complete=%s scanned=%,d appended=%,d skippedKnown=%,d "
                        + "skippedNonParquet=%,d skippedNonRegular=%,d elapsedMs=%,d%n",
                completed,
                scanned,
                appended,
                skippedKnown,
                skippedNonParquet,
                skippedNonRegular,
                Duration.between(startedAt, Instant.now()).toMillis()
        );
    }

    public static final class BootstrapState {
        public String startedAt;
        public String lastRunStartedAt;
        public String updatedAt;
        public String completedAt;
        public String finalizedAt;
        public String stopReason;
        public String inputDir;
        public String buildingManifestFile;
        public String finalManifestFile;
        public String currentManifestFile;
        public boolean completed;
        public long totalAppended;
        public long lastRunScanned;
        public long lastRunAppended;
        public long lastRunSkippedKnown;
        public long lastRunSkippedNonParquet;
        public long lastRunSkippedNonRegular;
        public String lastAppendedPath;
        public String lastAppendedModifiedAt;
    }
}
