package com.slack.kaldb;

import com.slack.kaldb.logstore.LuceneIndexStoreImpl;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.openjdk.jmh.annotations.*;

@State(Scope.Thread)
public class DuckDbBenchmark {
  private final Duration commitInterval = Duration.ofSeconds(5 * 60);
  private final Duration refreshInterval = Duration.ofSeconds(5 * 60);

  private Path tempDirectory;
  private MeterRegistry registry;
  LuceneIndexStoreImpl logStore;

  private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-mm-ddHH:mm:ss.SSSzzz");

  private DuckDBConnection conn;
  private int extraFields = 100;

  @Param({"100", "200", "300", "400"})
  private int iterations;

  @Setup(Level.Trial)
  public void createIndexer() throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    String filepath = "jdbc:duckdb:/tmp/dtest" + System.currentTimeMillis() + ".db";
    System.out.println("using file path " + filepath);
    conn = (DuckDBConnection) DriverManager.getConnection(filepath);
    Statement stmt = conn.createStatement();
    String tblCreation = "";
    for (int i = 0; i < extraFields; i++) {
      tblCreation += ", field" + i;
      tblCreation += " VARCHAR";
    }
    String createSql =
        "CREATE TABLE items (item VARCHAR, value DECIMAL(10,2), count INTEGER" + tblCreation + ")";
    System.out.println("create sql " + createSql);
    stmt.execute(createSql);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException, SQLException {
    //    Files.delete(Paths.get("/tmp/dtest5.db"));
    try {
      Statement stmt = conn.createStatement();
      try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM items")) {
        rs.next();
        System.out.println("row count:" + rs.getInt(1));
      }

      conn.close();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  private AtomicInteger atomicI = new AtomicInteger(0);

  @Benchmark
  public void measureLogSearcherSearch() throws SQLException {
    DuckDBAppender appender = conn.createAppender("main", "items");
    for (int j = 0; j < iterations; j++) {
      appender.beginRow();
      appender.append("test" + atomicI.incrementAndGet());
      appender.append(atomicI.incrementAndGet());
      appender.append(atomicI.incrementAndGet());
      for (int i = 0; i < extraFields; i++) {
        appender.append(null);
      }
      appender.endRow();
    }
    appender.flush();
    appender.close();
  }
}
