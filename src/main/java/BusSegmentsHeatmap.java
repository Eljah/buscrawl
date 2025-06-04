import org.apache.spark.sql.*;
import org.jfree.chart.*;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.PaintScaleLegend;
import org.jfree.chart.ui.RectangleEdge;
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

        Dataset<Row> speedStats = spark.read().parquet("output/speedStats.parquet");

// Сортируем датасет по speed_ratio по возрастанию
        List<Row> segments = speedStats.select(
                "start_stop_lat", "start_stop_lon",
                "end_stop_lat", "end_stop_lon", "speed_ratio",
                "start_name", "end_name"
        ).orderBy("speed_ratio").collectAsList();


        XYSeriesCollection dataset = new XYSeriesCollection();

        double minRatio = segments.stream().mapToDouble(r -> r.getDouble(4)).min().orElse(1);
        double maxRatio = segments.stream().mapToDouble(r -> r.getDouble(4)).max().orElse(1);

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setDefaultShapesVisible(false); // Убирает точки (shapes)
        renderer.setDefaultStroke(new BasicStroke(2.0f)); // Толщина линии

        // Создание шкалы цветов
        LookupPaintScale paintScale = new LookupPaintScale(minRatio, maxRatio, Color.blue);
        for (double val = minRatio; val <= maxRatio; val += (maxRatio - minRatio) / 100) {
            paintScale.add(val, getHeatMapColor(val, minRatio, maxRatio));
        }

        // Добавляем линии без подписей
        int seriesIndex = 0;
        for (Row segment : segments) {
            double startLat = segment.getAs("start_stop_lat");
            double startLon = segment.getAs("start_stop_lon");
            double endLat = segment.getAs("end_stop_lat");
            double endLon = segment.getAs("end_stop_lon");
            double speedRatio = segment.getAs("speed_ratio");
            String startName = segment.getAs("start_name");
            String endName = segment.getAs("end_name");
            String label = startName + " → " + endName;

            XYSeries series = new XYSeries(label, false); // false позволяет иметь одинаковые имена
            seriesIndex++;
            series.add(startLon, startLat);
            series.add(endLon, endLat);
            dataset.addSeries(series);

            renderer.setSeriesPaint(seriesIndex - 1, paintScale.getPaint(speedRatio));
        }

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Тепловая карта скоростей сегментов автобусов",
                "Долгота",
                "Широта",
                dataset
        );

        XYPlot plot = chart.getXYPlot();
        plot.setRenderer(renderer);

        // Добавляем градиентную шкалу (легенду)
        NumberAxis scaleAxis = new NumberAxis("Speed Ratio");
        scaleAxis.setRange(minRatio, maxRatio);
        scaleAxis.setTickLabelFont(new Font("Dialog", Font.PLAIN, 10));
        PaintScaleLegend psl = new PaintScaleLegend(paintScale, scaleAxis);
        psl.setPosition(RectangleEdge.RIGHT);
        psl.setMargin(5, 5, 5, 5);
        psl.setAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
        chart.addSubtitle(psl);

        // Настройка окна и отображение
        JFrame frame = new JFrame("Bus Segments Heatmap");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.add(new ChartPanel(chart));
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);

        spark.stop();
    }

    // Цветовая шкала от синего (низкие значения) до красного (высокие)
    private static Color getHeatMapColor(double value, double min, double max) {
        float ratio = (float) ((value - min) / (max - min));
        return Color.getHSBColor((0.7f - ratio * 0.7f), 1.0f, 1.0f);
    }
}
