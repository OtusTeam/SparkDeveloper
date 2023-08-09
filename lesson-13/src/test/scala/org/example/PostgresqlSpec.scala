package org.example

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager
import java.util.Properties

class PostgresqlSpec extends AnyFlatSpec with TestContainerForAll {

  override val containerDef = PostgreSQLContainer.Def()

  val testTableName = "users"

  "PostgreSQL data source" should "read table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate()

    spark
      .read
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .load()
      .show()

    spark.stop()
  }

  "PostgreSQL data source" should "write table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresWriterJob")
      .getOrCreate()

    import spark.implicits._

    val df = (60 to 70).map(_.toLong).toDF("user_id")

    df
      .write
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case c: PostgreSQLContainer => {
        val conn = connection(c)
        val stmt1 = conn.createStatement
        stmt1.execute(Queries.createTableQuery)
        val stmt2 = conn.createStatement
        stmt2.execute(Queries.insertDataQuery)
        conn.close()
      }
    }
  }

  def connection(c: PostgreSQLContainer) = {
    Class.forName(c.driverClassName)
    val properties = new Properties()
    properties.put("user", c.username)
    properties.put("password", c.password)
    DriverManager.getConnection(c.jdbcUrl, properties)
  }

  object Queries {
    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"

    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")

    lazy val insertDataQuery = s"INSERT INTO $testTableName VALUES $testValues;"
  }

}
