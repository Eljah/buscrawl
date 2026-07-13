import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public class BusRawParquetManifestBootstrapJob {
    public static void main(String[] args) throws Exception {
        Path inputDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path manifestFile = Path.of(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_FILE",
                inputDir.getParent().resolve("bus-data-parquet-legacy-manifest.tsv").toString()
        ));
        boolean force = Boolean.parseBoolean(System.getenv().getOrDefault(
                "BUS_RAW_PARQUET_MANIFEST_BOOTSTRAP_FORCE",
                "false"
        ));

        if (!Files.exists(inputDir)) {
            throw new IllegalStateException("Raw parquet directory not found: " + inputDir);
        }
        if (Files.exists(manifestFile) && !force) {
            System.out.println("BusRawParquetManifestBootstrapJob: manifest already exists: " + manifestFile);
            return;
        }

        Path tempManifest = manifestFile.resolveSibling(manifestFile.getFileName() + ".tmp");
        Files.deleteIfExists(tempManifest);
        Files.createDirectories(manifestFile.getParent());

        List<IncrementalParquetSupport.ParquetFileInfo> files = listParquetFilesWithFind(inputDir);
        files.sort(Comparator
                .comparing((IncrementalParquetSupport.ParquetFileInfo file) -> IncrementalParquetSupport.parseInstant(file.modifiedAt))
                .thenComparing(file -> file.path));
        BusRawParquetManifest.append(tempManifest, files);
        Files.move(tempManifest, manifestFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        System.out.printf(
                "BusRawParquetManifestBootstrapJob: wrote %d raw parquet records to %s%n",
                files.size(),
                manifestFile
        );
    }

    private static List<IncrementalParquetSupport.ParquetFileInfo> listParquetFilesWithFind(Path inputDir) throws Exception {
        Process process = new ProcessBuilder(
                "find",
                inputDir.toAbsolutePath().toString(),
                "-ignore_readdir_race",
                "-maxdepth",
                "1",
                "-type",
                "f",
                "-name",
                "*.parquet",
                "-printf",
                "%T@ %p\\0"
        ).start();

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        process.getInputStream().transferTo(stdout);
        process.getErrorStream().transferTo(stderr);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("find failed with exit code " + exitCode + ": "
                    + stderr.toString(StandardCharsets.UTF_8));
        }

        List<IncrementalParquetSupport.ParquetFileInfo> result = new ArrayList<>();
        for (String record : stdout.toString(StandardCharsets.UTF_8).split("\\u0000")) {
            if (record.isBlank()) {
                continue;
            }
            int separator = record.indexOf(' ');
            if (separator <= 0 || separator >= record.length() - 1) {
                continue;
            }
            double modifiedAtSeconds = Double.parseDouble(record.substring(0, separator));
            long epochSecond = (long) modifiedAtSeconds;
            long nanos = Math.round((modifiedAtSeconds - epochSecond) * 1_000_000_000L);

            IncrementalParquetSupport.ParquetFileInfo file = new IncrementalParquetSupport.ParquetFileInfo();
            file.modifiedAt = Instant.ofEpochSecond(epochSecond, nanos).toString();
            file.path = Path.of(record.substring(separator + 1)).toAbsolutePath().toString();
            result.add(file);
        }
        System.out.printf(Locale.US, "BusRawParquetManifestBootstrapJob: listed %,d raw parquet files%n", result.size());
        return result;
    }
}
