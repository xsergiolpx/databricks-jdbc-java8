package com.example.dbxjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class App {
  public static void main(String[] args) {
    String url =
      "jdbc:databricks://adb-2690017451936431.11.azuredatabricks.net:443;" +
      "HttpPath=/sql/1.0/warehouses/5b8213d5c7a1ef85";

    String token = System.getenv("DBX_TOKEN");
    if (token == null || token.trim().isEmpty()) {
      System.err.println("DBX_TOKEN env var is not set");
      System.exit(1);
    }

    Properties props = new Properties();
    props.put("PWD", token);
try {
  Class.forName("com.databricks.client.jdbc.Driver");
} catch (ClassNotFoundException e) {
  e.printStackTrace();
  System.exit(3);
}
    try (Connection conn = DriverManager.getConnection(url, props)) {
      System.out.println("Connected!");

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery("SHOW SCHEMAS")) {

        while (rs.next()) {
          System.out.println(rs.getString(1));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      System.exit(2);
    }
  }
}

