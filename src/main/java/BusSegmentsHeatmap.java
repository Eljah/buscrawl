import org.apache.spark.sql.*;
import org.jfree.chart.*;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.*;

import javax.swing.*;
import java.awt.*;
import java.util.List;

public class BusSegmentsHeatmap {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BusSegmentsHeatmap")
                .master("local[*]")
                .getOrCreate();

        // Загрузка данных из обновлённого Parquet-файла
        Dataset<Row> speedStats = spark.read().parquet("output/speedStats.parquet");

        // Отбор нужных колонок с обновлёнными именами
        List<Row> segments = speedStats.select(
                "start_stop_lat", "start_stop_lon", "end_stop_lat", "end_stop_lon", "speed_ratio"
        ).collectAsList();

        // Создание набора данных для графика
        XYSeriesCollection dataset = new XYSeriesCollection();

        for (Row segment : segments) {
            double startLat = segment.getAs("start_stop_lat");
            double startLon = segment.getAs("start_stop_lon");
            double endLat = segment.getAs("end_stop_lat");
            double endLon = segment.getAs("end_stop_lon");
            double speedRatio = segment.getAs("speed_ratio");

            XYSeries series = new XYSeries(speedRatio);
            series.add(startLon, startLat);
            series.add(endLon, endLat);
            dataset.addSeries(series);
        }

        // Создание графика
        JFreeChart chart = ChartFactory.createXYLineChart(
                "Тепловая карта скоростей сегментов автобусов",
                "Долгота",
                "Широта",
                dataset
        );

        XYPlot plot = chart.getXYPlot();
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        // Настройка цветов по принципу тепловой карты
        double minRatio = segments.stream().mapToDouble(r -> r.getDouble(4)).min().orElse(1);
        double maxRatio = segments.stream().mapToDouble(r -> r.getDouble(4)).max().orElse(1);

        for (int i = 0; i < dataset.getSeriesCount(); i++) {
            double speedRatio = (double) dataset.getSeriesKey(i);
            renderer.setSeriesPaint(i, getHeatMapColor(speedRatio, minRatio, maxRatio));
            renderer.setSeriesStroke(i, new BasicStroke(2.0f));
            renderer.setSeriesShapesVisible(i, false);
        }

        plot.setRenderer(renderer);

        // Отображение графика в окне
        JFrame frame = new JFrame("Bus Segments Heatmap");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.add(new ChartPanel(chart));
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);

        spark.stop();
    }

    // Возвращает цвет по тепловой карте
    private static Color getHeatMapColor(double value, double min, double max) {
        float ratio = (float) ((value - min) / (max - min));
        return Color.getHSBColor(0.7f - ratio * 0.7f, 1.0f, 1.0f);
    }
}
