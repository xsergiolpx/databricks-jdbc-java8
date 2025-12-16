
# Databricks JDBC Java 8 Compatibility Test – Step-by-Step Guide

## Objective

Validate **Databricks JDBC driver compatibility with Java 8** by:

  * Building the dedicated **`jdk-8` branch** of the Databricks JDBC driver.
  * Running a simple Java application that connects to a **Databricks SQL Warehouse**.
  * Executing the application on a **Java 8 runtime**.

This guide documents the **exact steps** used.

-----

## Environment

  * **OS:** AWS EC2 Linux VM (Amazon Linux 2023)
  * **Compilation:** Java 17 JDK
  * **Execution:** Java 8 Runtime
  * **Build Tool:** Maven
  * **Network:** Access to Databricks SQL Warehouse

### Why Java 17 + Java 8?

Amazon Linux 2023 does not easily provide a Java 8 JDK via `yum`. Maven requires a JDK to compile (`javac`), so the strategy is:

1.  **Compile** with Java 17 (targeting Java 8 bytecode).
2.  **Run** with the Java 8 runtime to validate compatibility.

-----

## Part A – Build the Databricks JDBC Driver (Java 8)

### A1. Install Maven

```bash
sudo yum update -y
sudo yum install -y maven
mvn -version
```

### A2. Verify Java 8 Runtime Exists

Check the JVM directory:

```bash
ls /usr/lib/jvm/
```

**Expected example path:**
`/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/bin/java`

Verify the version:

```bash
/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/bin/java -version
```

### A3. Clone JDBC Repository and Checkout Java 8 Branch

```bash
git clone https://github.com/databricks/databricks-jdbc.git
cd databricks-jdbc
git checkout jdk-8
```

### A4. Build Driver (Skip Tests)

Some tests may fail depending on the environment. Build without running tests:

```bash
mvn clean package -DskipTests
```

**Verify Resulting JAR:**

```bash
ls target
```

*Example Output:* `databricks-jdbc-1.0.9-oss.jar`

-----

## Part B – Create a Java Demo Application

### B1. Generate Maven Project

```bash
cd ~
rm -rf dbx-jdbc-demo

mvn archetype:generate \
  -DgroupId=com.example.dbxjdbc \
  -DartifactId=dbx-jdbc-demo \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false

cd dbx-jdbc-demo
```

### B2. Configure Java 8 Bytecode Target

Edit `pom.xml` to target Java 1.8:

```xml
<properties>
  <maven.compiler.source>1.8</maven.compiler.source>
  <maven.compiler.target>1.8</maven.compiler.target>
</properties>
```

### B3. Install Custom JDBC JAR into Local Maven Repo

Install the JAR built in Part A into your local repository:

```bash
mvn install:install-file \
  -Dfile=/home/ec2-user/databricks-jdbc/target/databricks-jdbc-1.0.9-oss.jar \
  -DgroupId=com.databricks \
  -DartifactId=databricks-jdbc \
  -Dversion=1.0.9-oss \
  -Dpackaging=jar
```

*Note: Adjust the `-Dfile` path if your directory structure is different.*

### B4. Add JDBC Dependency

In `pom.xml`, add the following under `<dependencies>`:

```xml
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc</artifactId>
  <version>1.0.9-oss</version>
</dependency>
```

-----

## Part C – Java Test Program

### C1. Create App.java

Create or edit the main application file:

```bash
nano src/main/java/com/example/dbxjdbc/App.java
```

Paste the following code (**Replace Host and HttpPath with your values**):

```java
package com.example.dbxjdbc;

import java.sql.*;
import java.util.Properties;

public class App {
  public static void main(String[] args) {
    // TODO: Replace with your specific Databricks Host and Warehouse Path
    String url =
      "jdbc:databricks://adb-XXXX.azuredatabricks.net:443;" +
      "HttpPath=/sql/1.0/warehouses/XXXX";

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
      System.exit(2);
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
      System.exit(3);
    }
  }
}
```

-----

## Part D – Compile and Run

### D1. Compile with Java 17 JDK

Set `JAVA_HOME` to the JDK (Amazon Corretto 17) to ensure `javac` is available:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64
export PATH="$JAVA_HOME/bin:$PATH"

java -version
mvn -version

mvn clean package
```

### D2. Set Databricks Token

```bash
export DBX_TOKEN="dapi_xxx"
```

### D3. Run Using Java 8 Runtime

Build the runtime classpath and execute using the Java 8 binary explicitly:

```bash
# Build classpath file
mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt

# Run with Java 8
/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/bin/java \
  -cp "target/classes:$(cat cp.txt)" \
  com.example.dbxjdbc.App
```

**Expected Output:**

```text
Connected!
default
information_schema
system
...
```

*This confirms Java 8 compatibility of the Databricks JDBC driver.*

-----

## Troubleshooting

**`mvn` not found**

  * **Solution:** Install Maven (see Step A1).

**"No compiler is provided"**

  * **Cause:** Maven is running on a JRE (Runtime only).
  * **Solution:** Switch `JAVA_HOME` to the Java 17 JDK before compiling (see Step D1).

**"No suitable driver found"**

  * **Solution:**
    1.  Ensure `Class.forName("com.databricks.client.jdbc.Driver")` is present in the code.
    2.  Ensure the full runtime classpath is included (`dependency:build-classpath`).

**Apache Arrow error on Java 16+**

  * **Context:** If you accidentally run this on Java 17 runtime instead of Java 8, you may see access errors.
  * **Solution:** Add `--add-opens=java.base/java.nio=ALL-UNNAMED` to the java command, OR simply use the Java 8 runtime as instructed in this guide.

