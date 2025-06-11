import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.GrayPaintScale;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.data.xy.DefaultXYZDataset;

import javax.swing.JFrame;

public class BusSpeedHeatmapNoLegend {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BusSpeedHeatmapNoLegend")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data = spark.read().parquet("D:/bus-data-parquet/");
        //Dataset<Row> data = spark.read().parquet("bus-data-parquet/");
        data.createOrReplaceTempView("buses");

        Dataset<Row> avgSpeedByLocation = spark.sql(
                "SELECT ROUND(latitude, 4) AS latitude, " +
                        "ROUND(longitude, 4) AS longitude, " +
                        "AVG(speed) AS avg_speed " +
                        "FROM buses " +
                        //"WHERE realRouteNumber = '10А' AND eventTime > '2025-05-31 13:00:00' " +
                        //"AND speed IS NOT NULL " +
                        "GROUP BY ROUND(latitude, 4), ROUND(longitude, 4)"
        );

        Row[] rows = (Row[]) avgSpeedByLocation.collect();

        double[][] dataset = new double[3][rows.length];
        for (int i = 0; i < rows.length; i++) {
            dataset[0][i] = rows[i].getDouble(1); // longitude
            dataset[1][i] = rows[i].getDouble(0); // latitude
            dataset[2][i] = rows[i].getDouble(2); // avg_speed
        }

        plotHeatMap(dataset, "Тепловая карта средней скорости без легенды");

        spark.stop();
    }

    private static void plotHeatMap(double[][] data, String title) {
        DefaultXYZDataset dataset = new DefaultXYZDataset();
        dataset.addSeries("Средняя скорость", data);

        NumberAxis xAxis = new NumberAxis("Longitude");
        NumberAxis yAxis = new NumberAxis("Latitude");

        XYBlockRenderer renderer = new XYBlockRenderer();
        renderer.setBlockHeight(0.0005);
        renderer.setBlockWidth(0.0005);

        GrayPaintScale scale = new GrayPaintScale(0, 60);
        renderer.setPaintScale(scale);

        XYPlot plot = new XYPlot(dataset, xAxis, yAxis, renderer);
        JFreeChart chart = new JFreeChart(title, plot);
        chart.removeLegend();
        // Не добавляем цветовую легенду

        ChartPanel chartPanel = new ChartPanel(chart);
        JFrame frame = new JFrame("Heatmap");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setContentPane(chartPanel);
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }
}
