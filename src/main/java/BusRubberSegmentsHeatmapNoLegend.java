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

public class BusRubberSegmentsHeatmapNoLegend {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("BusRubberSegmentsHeatmapNoLegend")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> segments = spark.read().parquet("D:/parquet/segments_adj.parquet");
        segments.createOrReplaceTempView("segments");

        Dataset<Row> stats = spark.sql(
                "WITH ordered AS (" +
                        "SELECT *, " +
                        "LEAD(departure_time) OVER (PARTITION BY plate, routeId, direction ORDER BY departure_time) AS next_dep, " +
                        "LEAD(start_stop) OVER (PARTITION BY plate, routeId, direction ORDER BY departure_time) AS next_start " +
                    "FROM segments" +
                "), agg AS (" +
                    "SELECT start_stop, end_stop, " +
                        "FIRST(start_name) AS start_name, FIRST(end_name) AS end_name, " +
                        "FIRST(start_stop_lat) AS start_stop_lat, FIRST(start_stop_lon) AS start_stop_lon, " +
                        "FIRST(end_stop_lat) AS end_stop_lat, FIRST(end_stop_lon) AS end_stop_lon, " +
                        "SUM(duration_sec) AS moving_sec, " +
                        "SUM(CASE WHEN next_start = end_stop THEN unix_timestamp(next_dep) - unix_timestamp(arrival_time) ELSE 0 END) AS waiting_sec " +
                    "FROM ordered GROUP BY start_stop, end_stop" +
                ") " +
                "SELECT *, (waiting_sec / moving_sec) AS rubber_ratio FROM agg ORDER BY rubber_ratio DESC"
        );

        List<Row> topSegments = stats.collectAsList();

        XYSeriesCollection dataset = new XYSeriesCollection();
        Map<String, Integer> labelCount = new HashMap<>();

        double maxRatio = topSegments.stream().mapToDouble(r -> r.getDouble(10)).max().orElse(0);
        double minRatio = topSegments.stream().mapToDouble(r -> r.getDouble(10)).min().orElse(0);

        LookupPaintScale paintScale = new LookupPaintScale(minRatio, maxRatio, Color.blue);
        for (double val = minRatio; val <= maxRatio; val += (maxRatio - minRatio) / 100.0 + 0.0001) {
            paintScale.add(val, getHeatMapColor(val, minRatio, maxRatio));
        }

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setDefaultShapesVisible(false);
        renderer.setDefaultStroke(new BasicStroke(2.0f));

        int seriesIndex = 0;
        for (Row segment : topSegments) {
            double startLat = segment.getDouble(4);
            double startLon = segment.getDouble(5);
            double endLat = segment.getDouble(6);
            double endLon = segment.getDouble(7);
            double ratio = segment.getDouble(10);
            String startName = segment.getString(2);
            String endName = segment.getString(3);
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

            renderer.setSeriesPaint(seriesIndex, paintScale.getPaint(ratio));
            seriesIndex++;
        }

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Сегменты с резиной (без легенды)",
                "Долгота",
                "Широта",
                dataset
        );

        XYPlot plot = chart.getXYPlot();
        plot.setRenderer(renderer);
        chart.removeLegend();

        JFrame frame = new JFrame("Rubber Segments Heatmap");
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
