package com.dt.spark.cores

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._


/**
 * @title: DataSet
 * @description:
 * @author: jguo
 * @date: 2021/6/29
 */
object Movie_Users_Analyzer_DataSet {

  //case class 和 object 是并行的关系
  case class User(UserID:String, Gender:String, Age:String, OccupationID:String, Zip_Code:String)
  case class Rating(UserID:String, MovieID: String, Rating:Double, Timestamp:String)
  case class Movie(MovieID:String, Title:String, Genres:String)

  def main(args: Array[String]): Unit = {

    //设置打印日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[4]"
    var dataPath = "data/"
    //创建Spark集群上下文sc
    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer"))
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    //build SparkSession
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    //注意导入隐式转换
    import spark.implicits._

    val schemeForUsers = StructType(List(StructField("UserID", StringType, true)
      ,StructField("Gender", StringType, true)
      ,StructField("Age", StringType, true)
      ,StructField("OccupationID", StringType, true)
      ,StructField("Zip_Code", StringType, true)
    ))
    val usersRDDRows = usersRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,line(2).trim,line(3).trim,line(4).trim))
    val usersDataFrame: DataFrame = spark.createDataFrame(usersRDDRows, schemeForUsers)
    //DF -> DS 需要进行隐式转换
    val usersDateSet = usersDataFrame.as[User]

    val schemaForRatings = StructType(List(StructField("UserID", StringType, true)
      ,StructField("MovieID", StringType, true)
      ,StructField("Rating", DoubleType, true)
      ,StructField("Timestamp", StringType, true)
    ))
    val ratingsRDDRows = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim,line(1).trim,line(2).trim.toDouble,line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaForRatings)
    val ratingsDataSet = ratingsDataFrame.as[Rating]

    ratingsDataSet.filter(s" MovieID = 1193")  //这里能够直接指定MovieID的原因是DataFrame中该元数据信息
      .join(usersDateSet, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)



  }



}
