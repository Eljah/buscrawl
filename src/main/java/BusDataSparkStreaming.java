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

        // Подключение к сокету (TCP-серверу)
        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
                .as(Encoders.STRING());

        // Парсим JSON в DataFrame
        Dataset<Row> busData = lines.select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withColumn("eventTime",
                        to_timestamp(from_unixtime(col("ts"), "yyyy-MM-dd HH:mm:ss"))
                                .cast(DataTypes.TimestampType));

        // Запуск стриминга с подробными логами подключения и получения данных
        StreamingQuery query = null;
        try {
            query = busData.writeStream()
                    .foreachBatch((dataset, batchId) -> {
                        long count = dataset.count();
                        System.out.printf("🔄 Batch #%d успешно получен. Записей в батче: %d%n", batchId, count);
                        if (count > 0) {
                            dataset.show(false);
                            dataset.write()
                                    .mode("append")
                                    .parquet("bus-data-parquet");
                            System.out.printf("✅ Данные батча #%d успешно записаны в parquet%n", batchId);
                        } else {
                            System.out.printf("⚠️ В батче #%d данных нет%n", batchId);
                        }
                    })
                    .option("checkpointLocation", "bus-data-checkpoint")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }

        System.out.println("🚀 Spark Streaming успешно запущен и слушает порт 9999");

        query.awaitTermination();
    }
}
