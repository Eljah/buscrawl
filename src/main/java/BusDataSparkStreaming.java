import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
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

        System.out.println("⏳ Spark запущен, пытаемся подключиться к TCP-серверу...");

        // Схема для входящих JSON данных
        StructType schema = new StructType()
                .add("internalRouteId", DataTypes.StringType)
                .add("realRouteNumber", DataTypes.StringType)
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("speed", DataTypes.IntegerType)
                .add("plate", DataTypes.StringType)
                .add("timestamp", DataTypes.LongType)
                .add("readableTime", DataTypes.StringType);


        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        Dataset<Row> busData = lines.select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withColumn("eventTime",
                        to_timestamp(col("readableTime"), "yyyy-MM-dd HH:mm:ss")
                                .cast(DataTypes.TimestampType));

        try {
            busData.writeStream()
                    .foreachBatch((dataset, batchId) -> {
                        System.out.printf("🔄 Batch #%d успешно получен. Записей в батче: %d%n", batchId, dataset.count());
                        dataset.show(false);
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


        System.out.println("🚀 Spark Streaming успешно запущен и слушает порт 9999");

    }
}
