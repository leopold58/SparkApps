package com.dt.spark.cores

import org.apache.spark.sql.SparkSession

object JDBCTest {
//  val
  def main(args: Array[String]): Unit = {
    // IP host
    val dbServer = ""

    val db = "TestDB"

    val url = s"jdbc:sqlserver://$dbServer:1433;databaseName=$db"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    val query = "SELECT * FROM Test123.dbo.Inventory"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", url)
      .option("user", "SA")
      .option("password", "MyPassWord123")
      .option("dbtable", s"($query) as my_table")
      .load()

      jdbcDF.show()

      spark.stop()

  }
}
