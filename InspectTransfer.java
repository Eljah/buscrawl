import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class InspectTransfer {
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("InspectTransfer")
        .master("local[1]")
        .config("spark.sql.session.timeZone","UTC")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    Dataset<Row> df = spark.read().parquet("/home/eljah/data/buscrawl/transfer-potential/journeys");
    df.printSchema();
    Dataset<Row> b = df.filter(col("serviceDate").cast("string").equalTo("2026-06-19"))
        .filter(col("departureBucketMinute").equalTo(480));
    System.out.println("bucket rows=" + b.count());
    b.groupBy("originStopId","originStopName").count().sort(desc("count")).show(200,false);
    b.filter(col("destinationStopName").contains("Речной"))
        .select("originStopId","originStopName","destinationStopId","destinationStopName","totalJourneySeconds","routePattern")
        .show(200,false);
    b.filter(col("originStopName").contains("Тасма"))
        .groupBy("originStopId","originStopName")
        .agg(count("*").alias("destinations"), min("totalJourneySeconds"), max("totalJourneySeconds"))
        .show(100,false);
    b.filter(col("originStopName").contains("Тасма"))
        .filter(col("destinationStopName").contains("Речной"))
        .show(100,false);
    Dataset<Row> rc = spark.read().parquet("/home/eljah/data/buscrawl/transfer-potential/request-grid-counts")
        .filter(col("serviceDate").cast("string").equalTo("2026-06-19"))
        .filter(col("departureBucketMinute").equalTo(480));
    rc.show(20,false);
    spark.stop();
  }
}
