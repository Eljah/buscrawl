import java.sql.*;
public class CheckTmpFallback {
 public static void main(String[] a)throws Exception{Class.forName("org.duckdb.DuckDBDriver");try(Connection c=DriverManager.getConnection("jdbc:duckdb:")){try(Statement s=c.createStatement()){s.execute("INSTALL parquet");s.execute("LOAD parquet");}
 String base="/home/eljah/data/buscrawl/tmp-transfer-static-fallback/journeys/serviceDate=2026-06-21/departureBucketMinute=480/*.parquet";
 try(ResultSet rs=c.createStatement().executeQuery("select count(*),count(distinct destinationStopId) from read_parquet('"+base+"') where originStopId in ('12078','12112')")){while(rs.next())System.out.println("totals "+rs.getLong(1)+" "+rs.getLong(2));}
 String q="select originStopId,destinationStopId,destinationStopName,totalJourneySeconds,routePattern from read_parquet('"+base+"') where originStopId in ('12078','12112') and (destinationStopName like '%Привокз%' or destinationStopName like '%Мосто%' or destinationStopName like '%Дербыш%' or destinationStopName like '%Речн%') order by totalJourneySeconds limit 50";
 try(ResultSet rs=c.createStatement().executeQuery(q)){while(rs.next())System.out.println(rs.getString(1)+" -> "+rs.getString(2)+" "+rs.getString(3)+" sec="+rs.getInt(4)+" "+rs.getString(5));}
 }}
}
