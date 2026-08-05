import java.sql.*;
public class QueryTransfer {
  public static void main(String[] args) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
      String glob = "/home/eljah/data/buscrawl/transfer-potential/journeys/serviceDate=2026-06-21/departureBucketMinute=480/*.parquet";
      String[] patterns = {"%Речной%", "%Привокз%", "%Мостотряд%", "%Дербыш%", "%Тасма%"};
      for (String p: patterns) {
        try (PreparedStatement ps = c.prepareStatement("select originStopId, originStopName, destinationStopId, destinationStopName, totalJourneySeconds, transferCount, rideCount, routePattern, firstBoardAt, finalAlightAt from read_parquet(?) where (destinationStopName like ? or originStopName like ?) and originStopId in ('12078','12112') order by totalJourneySeconds limit 50")) {
          ps.setString(1, glob); ps.setString(2, p); ps.setString(3, p);
          ResultSet rs = ps.executeQuery();
          System.out.println("PATTERN " + p);
          int n = 0;
          while (rs.next()) {
            n++;
            System.out.printf("%s %s -> %s %s sec=%d tx=%d rides=%d route=%s board=%s alight=%s%n",
              rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getInt(5), rs.getInt(6), rs.getInt(7), rs.getString(8), rs.getTimestamp(9), rs.getTimestamp(10));
          }
          System.out.println("rows=" + n);
        }
      }
      try (PreparedStatement ps = c.prepareStatement("select count(*), count(distinct destinationStopId), min(totalJourneySeconds), max(totalJourneySeconds) from read_parquet(?) where originStopId in ('12078','12112')")) {
        ps.setString(1, glob);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) System.out.println("origin totals rows=" + rs.getLong(1) + " dests=" + rs.getLong(2) + " min=" + rs.getInt(3) + " max=" + rs.getInt(4));
      }
    }
  }
}
