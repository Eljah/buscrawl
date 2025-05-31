import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class BusDataSparkStreaming {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("BusDataStreaming")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Определяем схему JSON-данных
        StructType schema = new StructType()
                .add("internalRouteId", DataTypes.StringType)
                .add("realRouteNumber", DataTypes.StringType)
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("s", DataTypes.IntegerType)
                .add("r", DataTypes.BooleanType)
                .add("h", DataTypes.IntegerType)
                .add("u", DataTypes.StringType)
                .add("ts", DataTypes.LongType)
                .add("b", DataTypes.LongType)
                .add("d", DataTypes.IntegerType)
                .add("g", DataTypes.StringType)
                .add("px", DataTypes.DoubleType)
                .add("py", DataTypes.DoubleType)
                .add("l", DataTypes.StringType);

        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        // Преобразуем JSON-строки в структурированный DataFrame
        Dataset<Row> busData = lines.select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withColumn("eventTime",
                        to_timestamp(from_unixtime(col("ts"), "yyyy-MM-dd HH:mm:ss"))
                                .cast(DataTypes.TimestampType));

        // Записываем данные через foreachBatch
        try {
            busData.writeStream()
                    .foreachBatch((dataset, batchId) -> {
                        System.out.printf("🔄 Записываем batch #%d с %d записями%n", batchId, dataset.count());
                        dataset.write()
                                .mode("append")
                                .parquet("bus-data-parquet");
                    })
                    .option("checkpointLocation", "bus-data-checkpoint")
                    .start()
                    .awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
