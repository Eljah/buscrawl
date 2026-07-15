import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

public class BusCompactedParquetRewriteJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Path compactedDir = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_PARQUET_DIR",
                "./var/bus/bus-data-parquet-compacted"
        ));
        Path stateFile = Path.of(System.getenv().getOrDefault(
                "BUS_COMPACTED_REWRITE_STATE_FILE",
                compactedDir.resolve("rewrite-state.json").toString()
        ));
        String targetDate = System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_SERVICE_DATE", "").trim();
        int minFiles = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_MIN_FILES", "8"));
        int maxDatesPerRun = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_MAX_DATES_PER_RUN", "1"));
        int excludeRecentDays = Integer.parseInt(System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_EXCLUDE_RECENT_DAYS", "2"));
        long minSourceAgeMinutes = Long.parseLong(System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_MIN_SOURCE_AGE_MINUTES", "30"));
        boolean keepBackup = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_COMPACTED_REWRITE_KEEP_BACKUP", "false"));

        Files.createDirectories(compactedDir);
        Files.createDirectories(stateFile.getParent());
        recoverInterruptedRewrite(compactedDir);

        LocalDate maxAllowedDate = LocalDate.now(ZoneId.of("Europe/Moscow")).minusDays(excludeRecentDays);
        List<ServiceDateCandidate> candidates = targetDate.isEmpty()
                ? listCandidates(compactedDir, minFiles, minSourceAgeMinutes, maxAllowedDate)
                : List.of(readCandidate(compactedDir.resolve("serviceDate=" + targetDate), minSourceAgeMinutes));
        candidates = candidates.stream()
                .filter(candidate -> candidate.files.size() >= minFiles || !targetDate.isEmpty())
                .sorted(Comparator.comparing(candidate -> candidate.serviceDate))
                .limit(Math.max(1, maxDatesPerRun))
                .collect(Collectors.toList());

        if (candidates.isEmpty()) {
            System.out.printf(
                    "BusCompactedParquetRewriteJob: no serviceDate needs rewrite in %s minFiles=%d maxAllowedDate=%s%n",
                    compactedDir,
                    minFiles,
                    maxAllowedDate
            );
            return;
        }

        RewriteState state = loadState(stateFile);
        for (ServiceDateCandidate candidate : candidates) {
            rewriteServiceDate(compactedDir, candidate, keepBackup);
            state.updatedAt = Instant.now().toString();
            state.lastServiceDate = candidate.serviceDate;
            state.lastFileCount = candidate.files.size();
            state.lastTotalBytes = candidate.totalBytes;
            writeState(stateFile, state);
        }
    }

    private static List<ServiceDateCandidate> listCandidates(
            Path compactedDir,
            int minFiles,
            long minSourceAgeMinutes,
            LocalDate maxAllowedDate
    ) throws Exception {
        if (!Files.exists(compactedDir)) {
            return List.of();
        }
        List<ServiceDateCandidate> result = new ArrayList<>();
        try (var paths = Files.list(compactedDir)) {
            for (Path dir : paths
                    .filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith("serviceDate="))
                    .collect(Collectors.toList())) {
                String serviceDate = dir.getFileName().toString().substring("serviceDate=".length());
                LocalDate date;
                try {
                    date = LocalDate.parse(serviceDate);
                } catch (Exception ignored) {
                    continue;
                }
                if (date.isAfter(maxAllowedDate)) {
                    continue;
                }
                ServiceDateCandidate candidate = readCandidate(dir, minSourceAgeMinutes);
                if (candidate.files.size() >= minFiles) {
                    result.add(candidate);
                }
            }
        }
        return result;
    }

    private static ServiceDateCandidate readCandidate(Path serviceDateDir, long minSourceAgeMinutes) throws Exception {
        if (!Files.isDirectory(serviceDateDir) || !serviceDateDir.getFileName().toString().startsWith("serviceDate=")) {
            throw new IllegalArgumentException("Invalid serviceDate directory: " + serviceDateDir);
        }
        Instant maxModifiedAt = Instant.now().minusSeconds(Math.max(0L, minSourceAgeMinutes) * 60L);
        List<Path> files;
        try (var paths = Files.list(serviceDateDir)) {
            files = paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .filter(path -> {
                        try {
                            return Files.getLastModifiedTime(path).toInstant().isBefore(maxModifiedAt);
                        } catch (Exception exception) {
                            return false;
                        }
                    })
                    .sorted()
                    .collect(Collectors.toList());
        }
        long totalBytes = 0L;
        for (Path file : files) {
            totalBytes += Files.size(file);
        }
        ServiceDateCandidate candidate = new ServiceDateCandidate();
        candidate.serviceDate = serviceDateDir.getFileName().toString().substring("serviceDate=".length());
        candidate.dir = serviceDateDir;
        candidate.files = files;
        candidate.totalBytes = totalBytes;
        return candidate;
    }

    private static void rewriteServiceDate(Path compactedDir, ServiceDateCandidate candidate, boolean keepBackup) throws Exception {
        String runId = "rewrite-" + Instant.now().toEpochMilli() + "-" + UUID.randomUUID();
        Path tempRoot = compactedDir.resolve("_rewrite-tmp");
        Path backupRoot = compactedDir.resolve("_rewrite-backup");
        Path tempFile = tempRoot.resolve(runId).resolve("serviceDate=" + candidate.serviceDate + ".parquet");
        Path replacementDir = compactedDir.resolve("serviceDate=" + candidate.serviceDate + ".replacement-" + runId);
        Path backupDir = backupRoot.resolve("serviceDate=" + candidate.serviceDate + "." + runId);
        Files.createDirectories(tempFile.getParent());
        Files.createDirectories(replacementDir);
        Files.createDirectories(backupRoot);

        System.out.printf(
                Locale.US,
                "BusCompactedParquetRewriteJob: rewriting serviceDate=%s files=%d totalMb=%.2f%n",
                candidate.serviceDate,
                candidate.files.size(),
                candidate.totalBytes / 1024.0 / 1024.0
        );

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("COPY (SELECT * FROM read_parquet(" + parquetList(candidate.files)
                    + ", union_by_name=true, hive_partitioning=false)) TO '"
                    + sqlEscape(tempFile.toAbsolutePath().toString())
                    + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }

        Path replacementFile = replacementDir.resolve(runId + ".parquet");
        Files.move(tempFile, replacementFile, StandardCopyOption.ATOMIC_MOVE);
        deleteRecursively(tempRoot.resolve(runId));

        Files.move(candidate.dir, backupDir, StandardCopyOption.ATOMIC_MOVE);
        Files.move(replacementDir, candidate.dir, StandardCopyOption.ATOMIC_MOVE);

        long replacementBytes = Files.size(candidate.dir.resolve(replacementFile.getFileName()));
        System.out.printf(
                Locale.US,
                "BusCompactedParquetRewriteJob: replaced serviceDate=%s oldFiles=%d oldMb=%.2f newFiles=1 newMb=%.2f%n",
                candidate.serviceDate,
                candidate.files.size(),
                candidate.totalBytes / 1024.0 / 1024.0,
                replacementBytes / 1024.0 / 1024.0
        );
        if (!keepBackup) {
            deleteRecursively(backupDir);
        }
    }

    private static void recoverInterruptedRewrite(Path compactedDir) throws Exception {
        Path backupRoot = compactedDir.resolve("_rewrite-backup");
        if (!Files.isDirectory(backupRoot)) {
            return;
        }
        try (var paths = Files.list(backupRoot)) {
            for (Path backupDir : paths.filter(Files::isDirectory).collect(Collectors.toList())) {
                String name = backupDir.getFileName().toString();
                int marker = name.indexOf(".rewrite-");
                if (!name.startsWith("serviceDate=") || marker <= 0) {
                    continue;
                }
                Path targetDir = compactedDir.resolve(name.substring(0, marker));
                if (!Files.exists(targetDir)) {
                    System.out.printf(
                            "BusCompactedParquetRewriteJob: recovering missing %s from %s%n",
                            targetDir,
                            backupDir
                    );
                    Files.move(backupDir, targetDir, StandardCopyOption.ATOMIC_MOVE);
                }
            }
        }
    }

    private static String parquetList(List<Path> files) {
        return files.stream()
                .map(path -> "'" + sqlEscape(path.toAbsolutePath().toString()) + "'")
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String sqlEscape(String value) {
        return value.replace("'", "''");
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path current : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.deleteIfExists(current);
            }
        }
    }

    private static RewriteState loadState(Path stateFile) throws Exception {
        if (!Files.exists(stateFile)) {
            return new RewriteState();
        }
        return MAPPER.readValue(stateFile.toFile(), RewriteState.class);
    }

    private static void writeState(Path stateFile, RewriteState state) throws Exception {
        Path tempFile = Files.createTempFile(stateFile.getParent(), stateFile.getFileName().toString(), ".tmp");
        MAPPER.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static final class ServiceDateCandidate {
        String serviceDate;
        Path dir;
        List<Path> files;
        long totalBytes;
    }

    public static final class RewriteState {
        public String updatedAt;
        public String lastServiceDate;
        public int lastFileCount;
        public long lastTotalBytes;
    }
}
