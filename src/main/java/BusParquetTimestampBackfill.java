import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.greatest;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

public class BusParquetTimestampBackfill {
    private static final ZoneId CITY_ZONE = ZoneId.of(
            System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow")
    );

    public static void main(String[] args) throws Exception {
        Path inputDir = Path.of(System.getenv().getOrDefault("BUS_PARQUET_DIR", "./var/bus/bus-data-parquet"));
        Path outputDir = Path.of(System.getenv().getOrDefault(
                "BUS_PARQUET_MIGRATION_OUTPUT_DIR",
                inputDir.resolveSibling(inputDir.getFileName() + "-migrated").toString()
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_PARQUET_MIGRATION_SPARK_LOCAL_DIR",
                "./var/bus/parquet-migration-spark-temp"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_PARQUET_MIGRATION_SPARK_MASTER", "local[*]");
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_PARQUET_MIGRATION_PARTITIONS", "512"));

        Files.createDirectories(sparkLocalDir);
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalStateException("Parquet directory not found: " + inputDir);
        }
        if (Files.exists(outputDir)) {
            throw new IllegalStateException("Migration output directory already exists: " + outputDir);
        }

        List<ParquetFileInfo> inputFiles = listParquetFiles(inputDir);
        if (inputFiles.isEmpty()) {
            throw new IllegalStateException("No readable parquet files found in " + inputDir);
        }

        System.out.printf(
                "BusParquetTimestampBackfill: inputDir=%s files=%d outputDir=%s partitions=%d%n",
                inputDir,
                inputFiles.size(),
                outputDir,
                outputPartitions
        );

        SparkSession spark = SparkSession.builder()
                .appName("BusParquetTimestampBackfill")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_PARQUET_MIGRATION_DRIVER_MEMORY", "4g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_PARQUET_MIGRATION_EXECUTOR_MEMORY", "4g"))
                .config("spark.sql.session.timeZone", CITY_ZONE.getId())
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            Map<String, Long> fileModifiedSeconds = new HashMap<>(inputFiles.size());
            String[] filePaths = new String[inputFiles.size()];
            for (int i = 0; i < inputFiles.size(); i++) {
                ParquetFileInfo file = inputFiles.get(i);
                filePaths[i] = file.path;
                fileModifiedSeconds.put(file.path, file.modifiedAtEpochSecond);
            }

            Broadcast<Map<String, Long>> modifiedSecondsBroadcast =
                    JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(fileModifiedSeconds);

            spark.udf().register(
                    "busFileModifiedSec",
                    (String path) -> modifiedSecondsBroadcast.value().get(normalizeInputFilePath(path)),
                    DataTypes.LongType
            );

            Dataset<Row> dataset = spark.read()
                    .option("mergeSchema", "true")
                    .parquet(filePaths)
                    .withColumn("_inputFile", input_file_name())
                    .withColumn("_fileModifiedSec", callUDF("busFileModifiedSec", col("_inputFile")));

            Set<String> existingColumns = new LinkedHashSet<>(List.of(dataset.columns()));
            if (!existingColumns.contains("timestamp")) {
                throw new IllegalStateException("Input parquet is missing required column 'timestamp'");
            }
            if (!existingColumns.contains("readableTime")) {
                throw new IllegalStateException("Input parquet is missing required column 'readableTime'");
            }

            if (!existingColumns.contains("sourceTimestamp")) {
                dataset = dataset.withColumn("sourceTimestamp", lit(null).cast(DataTypes.LongType));
            }
            if (!existingColumns.contains("sourceReadableTime")) {
                dataset = dataset.withColumn("sourceReadableTime", lit(null).cast(DataTypes.StringType));
            }

            Column needsMigration = col("sourceTimestamp").isNull();

            dataset = dataset
                    .withColumn("_fileMaxTimestamp", max(when(needsMigration, col("timestamp"))).over(
                            org.apache.spark.sql.expressions.Window.partitionBy("_inputFile")
                    ))
                    .withColumn(
                            "_shiftSec",
                            when(
                                    needsMigration.and(col("_fileModifiedSec").isNotNull()).and(col("_fileMaxTimestamp").isNotNull()),
                                    greatest(lit(0L), col("_fileModifiedSec").minus(col("_fileMaxTimestamp")))
                            ).otherwise(lit(0L))
                    )
                    .withColumn("sourceTimestamp", when(needsMigration, col("timestamp")).otherwise(col("sourceTimestamp")))
                    .withColumn("sourceReadableTime", when(needsMigration, col("readableTime")).otherwise(col("sourceReadableTime")))
                    .withColumn("timestamp", when(needsMigration, col("timestamp").plus(col("_shiftSec"))).otherwise(col("timestamp")))
                    .withColumn("readableTime", date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("eventTime", expr("timestamp_seconds(timestamp)"));

            List<String> selectedColumns = new ArrayList<>();
            for (String column : dataset.columns()) {
                if (!column.startsWith("_")) {
                    selectedColumns.add(column);
                }
            }

            Dataset<Row> migrated = dataset.select(selectedColumns.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new));
            migrated = migrated.repartition(outputPartitions);

            migrated.write()
                    .mode("overwrite")
                    .parquet(outputDir.toAbsolutePath().toString());

            System.out.printf(
                    "BusParquetTimestampBackfill: migrated parquet written to %s%n",
                    outputDir.toAbsolutePath()
            );
        } finally {
            spark.stop();
        }
    }

    private static List<ParquetFileInfo> listParquetFiles(Path parquetDir) throws IOException {
        try (Stream<Path> files = Files.walk(parquetDir, FileVisitOption.FOLLOW_LINKS)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                    .map(BusParquetTimestampBackfill::toParquetFileInfo)
                    .filter(file -> file != null)
                    .sorted(Comparator.comparing(file -> file.path))
                    .collect(Collectors.toList());
        }
    }

    private static String normalizeInputFilePath(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        if (!value.startsWith("file:")) {
            return value;
        }
        return Path.of(URI.create(value)).toAbsolutePath().toString();
    }

    private static ParquetFileInfo toParquetFileInfo(Path path) {
        try {
            if (!isReadableParquetFile(path)) {
                return null;
            }

            FileTime modifiedAt = Files.getLastModifiedTime(path);
            ParquetFileInfo file = new ParquetFileInfo();
            file.path = path.toAbsolutePath().toString();
            file.modifiedAtEpochSecond = modifiedAt.toInstant().getEpochSecond();
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

    private static final class ParquetFileInfo {
        private String path;
        private long modifiedAtEpochSecond;
    }
}
