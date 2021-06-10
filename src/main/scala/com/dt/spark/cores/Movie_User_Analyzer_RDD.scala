package com.dt.spark.cores


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @title:
 * @description:
 * @author: jguo
 * @date: 2021/6/9
 */
object Movie_User_Analyzer_RDD {

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
    //读取数据，用什么方式读取数据-此处采用的是RDD
    val tagsRDD = sc.textFile(dataPath + "tags.dat")
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    /**
     * rating.dat的数据格式
     * UserID::MovieID::Rating::Timestamp
     * 用户 ID、电影 ID、评分数据、时间戳
     */
    println("所有电影平均评分最高的电影有：")
    //输出数据说明：(5.0,64275) 第一项为平均评分，第二项为电影的ID
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache()

    ratings.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
      .map(x => (x._2._1.toDouble / x._2._2 , x._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)

    println("所有电影中粉丝或者观看人数最多的电影：")
    //输出数据说明：(356,34457) 第一项为观看电影的人数，第二项为电影的ID
    ratings.map(x => (x._2,1)).reduceByKey(_+_)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2,x._1))
      .take(10)
      .foreach(println)

  }


}
