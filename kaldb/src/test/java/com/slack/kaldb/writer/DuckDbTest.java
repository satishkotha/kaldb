package com.slack.kaldb.writer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class DuckDbTest {
  @Test
  public void testInsert() throws SQLException, InterruptedException, ClassNotFoundException {
    Class.forName("org.duckdb.DuckDBDriver");
    Connection conn = DriverManager.getConnection("jdbc:duckdb:/tmp/dtest3.db");
    //    Properties ro_prop = new Properties();
    //    ro_prop.setProperty("duckdb.", "true");
    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10,2), count INTEGER)");
    // insert two items into the table
    stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)");
    try (ResultSet rs = stmt.executeQuery("SELECT * FROM items")) {
      while (rs.next()) {
        System.out.println(rs.getString(1));
        System.out.println(rs.getInt(3));
      }
    }

    Thread.sleep(60000);
  }
}
