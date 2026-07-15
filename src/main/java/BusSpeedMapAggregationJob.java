import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_utc_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.sum;

public class BusSpeedMapAggregationJob {
    public static void main(String[] args) throws Exception {
        Path trafficBehaviorDir = Path.of(System.getenv().getOrDefault(
                "BUS_TRAFFIC_BEHAVIOR_DIR",
                "./var/bus/traffic-behavior"
        ));
        Path rawParquetDir = Path.of(System.getenv().getOrDefault(
                "BUS_SPEED_MAP_RAW_PARQUET_DIR",
                System.getenv().getOrDefault("BUS_COMPACTED_PARQUET_DIR", "./var/bus/bus-data-parquet-compacted")
        ));
        Path outputRoot = Path.of(System.getenv().getOrDefault(
                "BUS_SPEED_MAP_DIR",
                "./var/bus/speed-map"
        ));
        Path sparkLocalDir = Path.of(System.getenv().getOrDefault(
                "BUS_SPEED_MAP_SPARK_LOCAL_DIR",
                "./var/bus/speed-map-spark-temp"
        ));
        String sparkMaster = System.getenv().getOrDefault("BUS_SPEED_MAP_SPARK_MASTER", "local[2]");
        int outputPartitions = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_OUTPUT_PARTITIONS", "24"));
        boolean buildSparkCoordinateBuckets = Boolean.parseBoolean(System.getenv().getOrDefault("BUS_SPEED_MAP_BUILD_SPARK_COORDINATE_BUCKETS", "false"));
        int heatmapBucketPixels = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_HEATMAP_BUCKET_PIXELS", "6"));
        int minZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_MIN_ZOOM", "11"));
        int maxZoom = Integer.parseInt(System.getenv().getOrDefault("BUS_SPEED_MAP_MAX_ZOOM", "15"));
        String cityTimezone = System.getenv().getOrDefault("BUS_CITY_TIMEZONE", "Europe/Moscow");

        Path segmentTripsDir = trafficBehaviorDir.resolve("segment-trips");
        if (!Files.exists(segmentTripsDir)) {
            throw new IllegalStateException("Segment trips directory not found: " + segmentTripsDir);
        }

        Files.createDirectories(outputRoot);
        Files.createDirectories(sparkLocalDir);

        SparkSession spark = SparkSession.builder()
                .appName("BusSpeedMapAggregationJob")
                .master(sparkMaster)
                .config("spark.local.dir", sparkLocalDir.toAbsolutePath().toString())
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.driver.memory", System.getenv().getOrDefault("BUS_SPEED_MAP_DRIVER_MEMORY", "4g"))
                .config("spark.executor.memory", System.getenv().getOrDefault("BUS_SPEED_MAP_EXECUTOR_MEMORY", "4g"))
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> trips = spark.read()
                    .parquet(segmentTripsDir.toAbsolutePath().toString())
                    .filter(col("avgSegmentSpeedKmh").isNotNull())
                    .filter(col("avgSegmentSpeedKmh").gt(0));

            Dataset<Row> dailyPoint = trips
                    .withColumn("pointLatitude", round(col("startStopLatitude").plus(col("endStopLatitude")).divide(2), 3))
                    .withColumn("pointLongitude", round(col("startStopLongitude").plus(col("endStopLongitude")).divide(2), 3))
                    .groupBy("serviceDate", "weekdayIso", "pointLatitude", "pointLongitude")
                    .agg(
                            count("*").alias("sampleCount"),
                            round(avg("avgSegmentSpeedKmh"), 2).alias("avgSpeedKmh"),
                            round(min("avgSegmentSpeedKmh"), 2).alias("minSpeedKmh"),
                            round(max("avgSegmentSpeedKmh"), 2).alias("maxSpeedKmh"),
                            max("endEnteredStopAt").alias("latestTripAt")
                    );

            Dataset<Row> dailyPhysicalSegment = trips.groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("physicalSegmentId"),
                            col("startStopId"),
                            col("startStopName"),
                            col("startStopLatitude"),
                            col("startStopLongitude"),
                            col("endStopId"),
                            col("endStopName"),
                            col("endStopLatitude"),
                            col("endStopLongitude")
                    )
                    .agg(
                            count("*").alias("sampleCount"),
                            round(avg("avgSegmentSpeedKmh"), 2).alias("avgSpeedKmh"),
                            round(min("avgSegmentSpeedKmh"), 2).alias("minSpeedKmh"),
                            round(max("avgSegmentSpeedKmh"), 2).alias("maxSpeedKmh"),
                            max("endEnteredStopAt").alias("latestTripAt")
                    );

            Dataset<Row> dailyRouteSegment = trips.groupBy(
                            col("serviceDate"),
                            col("weekdayIso"),
                            col("segmentId"),
                            col("internalRouteId"),
                            col("routeNumber"),
                            col("direction"),
                            col("startStopId"),
                            col("startStopName"),
                            col("startStopLatitude"),
                            col("startStopLongitude"),
                            col("endStopId"),
                            col("endStopName"),
                            col("endStopLatitude"),
                            col("endStopLongitude")
                    )
                    .agg(
                            count("*").alias("sampleCount"),
                            round(avg("avgSegmentSpeedKmh"), 2).alias("avgSpeedKmh"),
                            round(min("avgSegmentSpeedKmh"), 2).alias("minSpeedKmh"),
                            round(max("avgSegmentSpeedKmh"), 2).alias("maxSpeedKmh"),
                            max("endEnteredStopAt").alias("latestTripAt")
                    );

            Dataset<Row> dailyRoute = trips.groupBy("serviceDate", "weekdayIso", "routeNumber")
                    .agg(
                            count("*").alias("sampleCount"),
                            round(avg("avgSegmentSpeedKmh"), 2).alias("avgSpeedKmh"),
                            round(min("avgSegmentSpeedKmh"), 2).alias("minSpeedKmh"),
                            round(max("avgSegmentSpeedKmh"), 2).alias("maxSpeedKmh"),
                            max("endEnteredStopAt").alias("latestTripAt")
                    );

            writeDailyAndSummaries(dailyPoint, outputRoot, "speed-point", outputPartitions,
                    "pointLatitude", "pointLongitude");
            writeDailyAndSummaries(dailyPhysicalSegment, outputRoot, "speed-physical-segment", outputPartitions,
                    "physicalSegmentId", "startStopId", "startStopName", "startStopLatitude", "startStopLongitude",
                    "endStopId", "endStopName", "endStopLatitude", "endStopLongitude");
            writeDailyAndSummaries(dailyRouteSegment, outputRoot, "speed-route-segment", outputPartitions,
                    "segmentId", "internalRouteId", "routeNumber", "direction", "startStopId", "startStopName",
                    "startStopLatitude", "startStopLongitude", "endStopId", "endStopName", "endStopLatitude", "endStopLongitude");
            writeDailyAndSummaries(dailyRoute, outputRoot, "speed-route", outputPartitions, "routeNumber");

            if (buildSparkCoordinateBuckets && Files.exists(rawParquetDir)) {
                writeCoordinateHeatmapBuckets(
                        spark,
                        rawParquetDir,
                        outputRoot,
                        outputPartitions,
                        heatmapBucketPixels,
                        minZoom,
                        maxZoom,
                        cityTimezone
                );
            } else if (buildSparkCoordinateBuckets) {
                System.err.println("BusSpeedMapAggregationJob: raw parquet directory not found, skipping coordinate heatmap buckets: " + rawParquetDir);
            }
        } finally {
            spark.stop();
        }
    }

    private static void writeCoordinateHeatmapBuckets(
            SparkSession spark,
            Path rawParquetDir,
            Path outputRoot,
            int outputPartitions,
            int bucketPixels,
            int minZoom,
            int maxZoom,
            String cityTimezone
    ) {
        Dataset<Row> rawPoints = spark.read()
                .option("mergeSchema", "false")
                .parquet(rawParquetDir.toAbsolutePath().toString().replace('\\', '/') + "/**/*.parquet")
                .filter(col("latitude").isNotNull())
                .filter(col("longitude").isNotNull())
                .filter(col("eventTime").isNotNull())
                .filter(col("speed").isNotNull())
                .filter(col("latitude").between(54.0, 57.0))
                .filter(col("longitude").between(47.0, 52.0))
                .filter(col("speed").geq(0).and(col("speed").leq(140)))
                .withColumn("localEventTime", from_utc_timestamp(col("eventTime"), cityTimezone))
                .withColumn("serviceDate", col("localEventTime").cast("date"))
                .withColumn("weekdayIso", org.apache.spark.sql.functions.expr("((dayofweek(localEventTime) + 5) % 7) + 1"))
                .select("serviceDate", "weekdayIso", "latitude", "longitude", "speed");

        Dataset<Row> union = null;
        for (int zoom = minZoom; zoom <= maxZoom; zoom++) {
            double scale = 256.0 * Math.pow(2.0, zoom);
            String worldXExpr = "((longitude + 180.0) / 360.0) * " + scale;
            String worldYExpr = "(0.5 - log((1 + sin(radians(latitude))) / (1 - sin(radians(latitude)))) / (4 * pi())) * " + scale;
            Dataset<Row> buckets = rawPoints
                    .withColumn("zoom", lit(zoom))
                    .withColumn("bucketSizePx", lit(bucketPixels))
                    .withColumn("bucketX", org.apache.spark.sql.functions.expr("cast(floor((" + worldXExpr + ") / " + bucketPixels + ") as int)"))
                    .withColumn("bucketY", org.apache.spark.sql.functions.expr("cast(floor((" + worldYExpr + ") / " + bucketPixels + ") as int)"))
                    .groupBy("serviceDate", "weekdayIso", "zoom", "bucketSizePx", "bucketX", "bucketY")
                    .agg(
                            count("*").alias("sampleCount"),
                            round(avg("speed"), 2).alias("avgSpeedKmh"),
                            round(min("speed"), 2).alias("minSpeedKmh"),
                            round(max("speed"), 2).alias("maxSpeedKmh")
                    );
            union = union == null ? buckets : union.unionByName(buckets);
        }

        if (union == null) {
            return;
        }

        union.repartition(outputPartitions)
                .write()
                .mode("overwrite")
                .partitionBy("serviceDate")
                .parquet(outputRoot.resolve("daily-speed-coordinate-bucket").toAbsolutePath().toString());

        union.groupBy("zoom", "bucketSizePx", "bucketX", "bucketY")
                .agg(
                        sum("sampleCount").alias("totalSamples"),
                        count("*").alias("sampleDays"),
                        round(avg("avgSpeedKmh"), 2).alias("avgSpeedKmh"),
                        round(min("minSpeedKmh"), 2).alias("minSpeedKmh"),
                        round(max("maxSpeedKmh"), 2).alias("maxSpeedKmh")
                )
                .repartition(Math.max(1, outputPartitions / 2))
                .write()
                .mode("overwrite")
                .parquet(outputRoot.resolve("summary-speed-coordinate-bucket-all-days").toAbsolutePath().toString());

        union.groupBy("weekdayIso", "zoom", "bucketSizePx", "bucketX", "bucketY")
                .agg(
                        sum("sampleCount").alias("totalSamples"),
                        count("*").alias("sampleDays"),
                        round(avg("avgSpeedKmh"), 2).alias("avgSpeedKmh"),
                        round(min("minSpeedKmh"), 2).alias("minSpeedKmh"),
                        round(max("maxSpeedKmh"), 2).alias("maxSpeedKmh")
                )
                .repartition(Math.max(1, outputPartitions / 2))
                .write()
                .mode("overwrite")
                .parquet(outputRoot.resolve("summary-speed-coordinate-bucket-by-weekday").toAbsolutePath().toString());
    }

    private static void writeDailyAndSummaries(
            Dataset<Row> daily,
            Path outputRoot,
            String name,
            int outputPartitions,
            String... dimensions
    ) {
        daily.repartition(outputPartitions)
                .write()
                .mode("overwrite")
                .partitionBy("serviceDate")
                .parquet(outputRoot.resolve("daily-" + name).toAbsolutePath().toString());

        daily.groupBy(toColumns(dimensions))
                .agg(
                        sum("sampleCount").alias("totalSamples"),
                        count("*").alias("sampleDays"),
                        round(avg("avgSpeedKmh"), 2).alias("avgSpeedKmh"),
                        round(min("minSpeedKmh"), 2).alias("minSpeedKmh"),
                        round(max("maxSpeedKmh"), 2).alias("maxSpeedKmh"),
                        max("latestTripAt").alias("latestTripAt")
                )
                .repartition(Math.max(1, outputPartitions / 2))
                .write()
                .mode("overwrite")
                .parquet(outputRoot.resolve("summary-" + name + "-all-days").toAbsolutePath().toString());

        String[] weekdayDimensions = new String[dimensions.length + 1];
        weekdayDimensions[0] = "weekdayIso";
        System.arraycopy(dimensions, 0, weekdayDimensions, 1, dimensions.length);
        daily.groupBy(toColumns(weekdayDimensions))
                .agg(
                        sum("sampleCount").alias("totalSamples"),
                        count("*").alias("sampleDays"),
                        round(avg("avgSpeedKmh"), 2).alias("avgSpeedKmh"),
                        round(min("minSpeedKmh"), 2).alias("minSpeedKmh"),
                        round(max("maxSpeedKmh"), 2).alias("maxSpeedKmh"),
                        max("latestTripAt").alias("latestTripAt")
                )
                .repartition(Math.max(1, outputPartitions / 2))
                .write()
                .mode("overwrite")
                .parquet(outputRoot.resolve("summary-" + name + "-by-weekday").toAbsolutePath().toString());
    }

    private static org.apache.spark.sql.Column[] toColumns(String... names) {
        org.apache.spark.sql.Column[] columns = new org.apache.spark.sql.Column[names.length];
        for (int i = 0; i < names.length; i++) {
            columns[i] = col(names[i]);
        }
        return columns;
    }
}
