import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class IncrementalParquetSupport {
    private IncrementalParquetSupport() {
    }

    public static List<ParquetFileInfo> listRecentParquetFiles(Path parquetDir, Instant modifiedSince) throws IOException {
        FileTime modifiedSinceTime = FileTime.from(modifiedSince);
        try (Stream<Path> files = Files.walk(parquetDir, FileVisitOption.FOLLOW_LINKS)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .map(path -> toParquetFileInfo(path, modifiedSinceTime))
                    .filter(file -> file != null)
                    .sorted(Comparator
                            .comparing((ParquetFileInfo file) -> parseInstant(file.modifiedAt))
                            .thenComparing(file -> file.path))
                    .collect(Collectors.toList());
        }
    }

    public static List<ParquetFileInfo> selectNewFiles(
            List<ParquetFileInfo> candidateFiles,
            String lastProcessedModifiedAtText,
            String lastProcessedPath
    ) {
        Instant lastProcessedModifiedAt = parseInstant(lastProcessedModifiedAtText);
        List<ParquetFileInfo> newFiles = new ArrayList<>();
        for (ParquetFileInfo file : candidateFiles) {
            Instant fileModifiedAt = parseInstant(file.modifiedAt);
            if (fileModifiedAt == null) {
                continue;
            }
            if (lastProcessedModifiedAt == null || fileModifiedAt.isAfter(lastProcessedModifiedAt)) {
                newFiles.add(file);
                continue;
            }
            if (fileModifiedAt.equals(lastProcessedModifiedAt)
                    && lastProcessedPath != null
                    && file.path.compareTo(lastProcessedPath) > 0) {
                newFiles.add(file);
            }
        }
        return newFiles;
    }

    public static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static ParquetFileInfo toParquetFileInfo(Path path, FileTime modifiedSinceTime) {
        try {
            FileTime modifiedAt = Files.getLastModifiedTime(path);
            if (!isReadableParquetFile(path) || modifiedAt.compareTo(modifiedSinceTime) < 0) {
                return null;
            }
            ParquetFileInfo file = new ParquetFileInfo();
            file.path = path.toAbsolutePath().toString();
            file.modifiedAt = modifiedAt.toInstant().toString();
            return file;
        } catch (IOException e) {
            System.err.println("Skipping parquet file " + path + ": " + e.getMessage());
            return null;
        }
    }

    private static boolean isReadableParquetFile(Path path) {
        try {
            long size = Files.size(path);
            if (size < 8L) {
                return false;
            }

            byte[] header = new byte[4];
            byte[] footer = new byte[4];
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "r")) {
                file.readFully(header);
                file.seek(size - 4L);
                file.readFully(footer);
            }

            return isParquetMagic(header) && isParquetMagic(footer);
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean isParquetMagic(byte[] bytes) {
        return bytes.length == 4
                && bytes[0] == 'P'
                && bytes[1] == 'A'
                && bytes[2] == 'R'
                && bytes[3] == '1';
    }

    public static final class ParquetFileInfo {
        public String path;
        public String modifiedAt;
    }
}
