import org.apache.spark.sql.*;
import org.jfree.chart.*;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.*;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BusOvertakeSegmentsHeatmapNoLegend {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BusOvertakeSegmentsHeatmapNoLegend")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> segments = spark.read().parquet("D:/parquet/segments_adj.parquet");
        Dataset<Row> overtakes = spark.read().parquet("D:/parquet/overtakes.parquet");

        segments.createOrReplaceTempView("segments");
        overtakes.createOrReplaceTempView("overtakes");

        Dataset<Row> segmentCounts = spark.sql(
                "WITH cnt AS (" +
                        "SELECT start_stop, end_stop, COUNT(*) AS overtake_count " +
                        "FROM overtakes GROUP BY start_stop, end_stop" +
                    ") " +
                    "SELECT c.start_stop, c.end_stop, c.overtake_count, " +
                        "FIRST(s.start_stop_lat) AS start_stop_lat, FIRST(s.start_stop_lon) AS start_stop_lon, " +
                        "FIRST(s.end_stop_lat) AS end_stop_lat, FIRST(s.end_stop_lon) AS end_stop_lon, " +
                        "FIRST(s.start_name) AS start_name, FIRST(s.end_name) AS end_name " +
                    "FROM cnt c JOIN segments s ON c.start_stop = s.start_stop AND c.end_stop = s.end_stop " +
                    "GROUP BY c.start_stop, c.end_stop, c.overtake_count " +
                    "ORDER BY c.overtake_count DESC"
        );

        List<Row> topSegments = segmentCounts.collectAsList();

        XYSeriesCollection dataset = new XYSeriesCollection();
        Map<String, Integer> labelCount = new HashMap<>();

        int maxCount = topSegments.stream().mapToInt(r -> r.getInt(2)).max().orElse(1);
        int minCount = topSegments.stream().mapToInt(r -> r.getInt(2)).min().orElse(0);

        LookupPaintScale paintScale = new LookupPaintScale(minCount, maxCount, Color.blue);
        for (double val = minCount; val <= maxCount; val += (maxCount - minCount) / 100.0 + 0.0001) {
            paintScale.add(val, getHeatMapColor(val, minCount, maxCount));
        }

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setDefaultShapesVisible(false);
        renderer.setDefaultStroke(new BasicStroke(2.0f));

        int seriesIndex = 0;
        for (Row segment : topSegments) {
            double startLat = segment.getDouble(3);
            double startLon = segment.getDouble(4);
            double endLat = segment.getDouble(5);
            double endLon = segment.getDouble(6);
            int count = segment.getInt(2);
            String startName = segment.getString(7);
            String endName = segment.getString(8);
            String label = startName + " → " + endName;

            int cnt = labelCount.getOrDefault(label, 0);
            labelCount.put(label, cnt + 1);
            if (cnt > 0) {
                label = label + " (" + cnt + ")";
            }

            XYSeries series = new XYSeries(label, false);
            series.add(startLon, startLat);
            series.add(endLon, endLat);
            dataset.addSeries(series);

            renderer.setSeriesPaint(seriesIndex, paintScale.getPaint(count));
            seriesIndex++;
        }

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Сегменты с обгонами (без легенды)",
                "Долгота",
                "Широта",
                dataset
        );

        XYPlot plot = chart.getXYPlot();
        plot.setRenderer(renderer);
        chart.removeLegend();

        JFrame frame = new JFrame("Overtake Segments Heatmap");
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.setSize(800, 600);
        frame.add(new ChartPanel(chart));
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);

        spark.stop();
    }

    private static Color getHeatMapColor(double value, double min, double max) {
        float ratio = (float) ((value - min) / (max - min));
        return Color.getHSBColor((0.7f - ratio * 0.7f), 1.0f, 1.0f);
    }
}
