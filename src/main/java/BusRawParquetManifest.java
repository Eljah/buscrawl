import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class BusRawParquetManifest {
    private BusRawParquetManifest() {
    }

    public static void append(Path manifestFile, List<IncrementalParquetSupport.ParquetFileInfo> files) throws IOException {
        if (files.isEmpty()) {
            return;
        }
        Files.createDirectories(manifestFile.getParent());
        List<IncrementalParquetSupport.ParquetFileInfo> sorted = new ArrayList<>(files);
        sorted.sort(Comparator
                .comparing((IncrementalParquetSupport.ParquetFileInfo file) -> IncrementalParquetSupport.parseInstant(file.modifiedAt))
                .thenComparing(file -> file.path));
        try (BufferedWriter writer = Files.newBufferedWriter(
                manifestFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            for (IncrementalParquetSupport.ParquetFileInfo file : sorted) {
                writer.write(file.modifiedAt);
                writer.write('\t');
                writer.write(file.path);
                writer.write('\n');
            }
        }
    }

    public static ManifestWindow readWindow(Path manifestFile, long offset, int maxRecords) throws IOException {
        ManifestWindow result = new ManifestWindow();
        result.nextOffset = Math.max(0L, offset);
        result.endOffset = result.nextOffset;
        if (!Files.exists(manifestFile) || maxRecords <= 0) {
            result.reachedEnd = true;
            return result;
        }
        try (RandomAccessFile file = new RandomAccessFile(manifestFile.toFile(), "r")) {
            result.endOffset = file.length();
            long safeOffset = Math.min(Math.max(0L, offset), file.length());
            file.seek(safeOffset);
            String line;
            while (result.records.size() < maxRecords && (line = file.readLine()) != null) {
                result.nextOffset = file.getFilePointer();
                IncrementalParquetSupport.ParquetFileInfo info = parseLine(line);
                if (info != null) {
                    result.records.add(info);
                }
            }
            result.reachedEnd = result.nextOffset >= file.length();
        }
        return result;
    }

    public static List<IncrementalParquetSupport.ParquetFileInfo> readAll(Path manifestFile) throws IOException {
        List<IncrementalParquetSupport.ParquetFileInfo> result = new ArrayList<>();
        if (!Files.exists(manifestFile)) {
            return result;
        }
        try (BufferedReader reader = Files.newBufferedReader(manifestFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                IncrementalParquetSupport.ParquetFileInfo info = parseLine(line);
                if (info != null) {
                    result.add(info);
                }
            }
        }
        return result;
    }

    public static IncrementalParquetSupport.ParquetFileInfo toInfo(Path path) throws IOException {
        IncrementalParquetSupport.ParquetFileInfo info = new IncrementalParquetSupport.ParquetFileInfo();
        info.path = path.toAbsolutePath().toString();
        info.modifiedAt = Files.getLastModifiedTime(path).toInstant().toString();
        return info;
    }

    private static IncrementalParquetSupport.ParquetFileInfo parseLine(String line) {
        int separator = line.indexOf('\t');
        if (separator <= 0 || separator >= line.length() - 1) {
            return null;
        }
        String modifiedAt = line.substring(0, separator);
        Instant parsed = IncrementalParquetSupport.parseInstant(modifiedAt);
        if (parsed == null) {
            return null;
        }
        IncrementalParquetSupport.ParquetFileInfo info = new IncrementalParquetSupport.ParquetFileInfo();
        info.modifiedAt = parsed.toString();
        info.path = line.substring(separator + 1);
        return info;
    }

    public static final class ManifestWindow {
        public final List<IncrementalParquetSupport.ParquetFileInfo> records = new ArrayList<>();
        public long nextOffset;
        public long endOffset;
        public boolean reachedEnd;
    }
}
