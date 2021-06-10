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

    println("所有电影平均评分最高的电影有：")
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0),x(1),x(2))).cache()




  }


}
