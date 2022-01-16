package com.dt.spark.cores

import org.apache.spark.sql.SparkSession

object JDBCTest {
//  val
  def main(args: Array[String]): Unit = {
    val dbServer = "172.16.244.158"

    val db = "TestDB"

    val url = s"jdbc:sqlserver://$dbServer:1433;databaseName=$db"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    val query = "SELECT * FROM Inventory"
    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", url)
      .option("user", "SA")
      .option("password", "MyPassWord123")
//      .option("dbtable", "Inventory")
//      .option("dbtable", "SELECT * FROM Inventory GO")
      .option("dbtable", s"($query) as my_table")
      .load()

      jdbcDF.show()

      spark.stop()

  }
}
