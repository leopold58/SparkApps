package com.dt.spark.cores

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title: 电影点评系统用户行为分析
 * @description: 使用Spark SQL来实现用户行为分析，通过DataFrame来实现某特定电影观看者中男性和女性不同年龄人数
 * @author: jguo
 * @date: 2021/6/20
 */
object Movie_Users_Analyzer_DateFrame {
  /**
   * users.dat
   *  UserID::Gender::Age::Occupation::Zip-code
   *  用户ID、性别、年龄、职业、邮编代码
   * ratings.dat
   *  UserID::MovieID::Rating::Timestamp
   *  用户 ID、电影ID、评分数据、时间戳
   * movies.dat
   *  MovieID::Title::Genres
   *  电影 ID、电影名、电影类型
   */
  def main(args: Array[String]): Unit = {

    //设置打印日志的输出级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    var masterUrl = "local[4]"
    var dataPath = "data/"
    if(args.length > 0 ){
      masterUrl = args(0)
    }else if(args.length > 1){
      dataPath = args(1)
    }
    //创建Spark集群上下文sc
    val sc = new SparkContext(new SparkConf().setMaster(masterUrl).setAppName("Movie_Users_Analyzer"))
    val usersRDD = sc.textFile(dataPath + "users.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val occupationsRDD = sc.textFile(dataPath + "occupations.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    //build SparkSession
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    //创建schema，即DF所对应的元数据信息
    val schemaForUsers = StructType(List(StructField("UserID", StringType, true)
      , StructField("Gender", StringType, true)
      , StructField("Age", StringType, true)
      , StructField("OccupationID", StringType, true)
      , StructField("Zip-code", StringType, true)
    ))

    //把Users的数据格式化，拆开一一对应
    val usersRDDRows = usersRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim,line(2).trim,line(3).trim,line(4).trim))
    // 创建df
    val usersDataFrame = spark.createDataFrame(usersRDDRows,schemaForUsers)

    val schemaForRatings = StructType(List(StructField("UserID", StringType, true)
      ,StructField("MovieID", StringType, true)
      ,StructField("Rating", DoubleType, true)
      ,StructField("Timestamp", StringType, true)
    ))
    val ratingsRDDRows = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame: DataFrame = spark.createDataFrame(ratingsRDDRows, schemaForRatings)

    val schemaForMovies = StructType(List(StructField("MovieID", StringType, true)
      ,StructField("Title", StringType, true)
      ,StructField("Genres", StringType, true)
    ))
    val moviesRDDRows = moviesRDD.map(_.split("::")).map(line => Row(line(0).trim,line(1).trim,line(2).trim))
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows,schemaForMovies)

    println("功能一：通过DataFrame实现某特定电影观看者中男性和女性不同年龄分别有什么人？")
    ratingsDataFrame.filter(s"MovieID = 1193")
      .join(usersDataFrame, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)

    println("功能二：用GlobalTempView的sql语句实现某特定电影观看者中男性和女性不同年龄分别有多少人？")
    ratingsDataFrame.createGlobalTempView("ratings")
    usersDataFrame.createGlobalTempView("users")
    spark.sql(
      s"""
         |select Gender,Age,count(*)
         |from global_temp.users u
         |join global_temp.ratings r
         |on u.UserID = r.UserID
         |where MovieID = 1193
         |group by Gender, Age
         |""".stripMargin).show(10)
    println("功能二：用LocalTempView的sql语句实现某特定观看者中男性和女性不同年龄分别有多少人？")
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    spark.sql(
      s"""
         |select Gender,Age,count(*)
         |from users u
         |join ratings as r
         |on u.UserID = r.UserID
         |where MovieID = 1193
         |group by Gender,Age
         |""".stripMargin).show(10)

    println("纯粹通过RDD的方式实现所有电影中平均得分最高的电影TopN：")
    val ratings = ratingsRDD.map(_.split("::")).map( x => (x(0),x(1),x(2))).cache()
    ratings.map(x => (x._2, (x._3.toDouble,1.toDouble)))
      .reduceByKey((x,y)=>(x._1 + y._1,x._2 + y._2))
      .map(x => (x._2._1.toDouble/x._2._2.toDouble,x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .foreach(println)

    ratingsDataFrame.printSchema()

    println("通过DataFrame和RDD相结合的方式计算所有电影中平均得分最高（口碑最好）的电影TopN：")
    ratingsDataFrame.select("MovieID", "Rating").groupBy("MovieID")
      .avg("Rating")
      .rdd
      .map(row => (row(1),(row(0),row(1))))
      .sortBy(_._1.toString.toDouble, false)
      .map(tuple => tuple._2)
      .collect
      .take(10)
      .foreach(println)

    import spark.sqlContext.implicits._
    println("通过纯粹使用DataFrame方式计算所有电影中平均得分最高（口碑最好）的电影TopN：")
    ratingsDataFrame.select("MovieID", "Rating").groupBy("MovieID")
      .avg("Rating")
      .orderBy($"avg(Rating)".desc)
      .show(10)

    println("纯粹通过RDD的方式计算所有电影中粉丝或观看人数最多（最流行电影）的电影TopN：")
    ratings.map(x => (x._2,1))
      .reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1)).collect()
      .take(10)
      .foreach(println)

    println("通过DataFrame 和 RDD结合的方式计算最流行电影（即所有电影中粉丝或者观看人数最多）的电影TopN：")
    ratingsDataFrame.select("MovieID", "Timestamp").groupBy("MovieID")
      .count()
      .rdd
      .map(row => (row(1).toString.toLong, (row(0), row(1))))
      .sortByKey(false)
      .map(tuple => tuple._2).collect()
      .take(10)
      .foreach(println)

    println("纯粹通过DataFrame的方式计算最流行电影（即所有电影中粉丝或者观看人数最多）的电影TopN：")
    ratingsDataFrame.groupBy("MovieID").count()
      .orderBy($"count".desc).show(10)

    /**
     *  上面 纯碎通过DataFrame的方式计算最流行电影的输出
     *  |MovieID|count|
        +-------+-----+
        |   2858| 3428|
        |    260| 2991|
        |   1196| 2990|
        |   1210| 2883|
        |    480| 2672|
        |   2028| 2653|
        |    589| 2649|
        |   2571| 2590|
        |   1270| 2583|
        |    593| 2578|
     */

  }


}
