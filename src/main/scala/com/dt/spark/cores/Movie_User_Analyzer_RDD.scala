package com.dt.spark.cores


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

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
    val usersRDD = sc.textFile(dataPath + "users.dat")
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

    /**
     * users.dat数据格式
     * UserID::Gender::Age::Occupation::Zip-code
     * 用户ID、性别、年龄、职业、邮编代码
     */
    println("统计男性和女性最喜欢的电影")
    val male = "M"
    val female = "F"
    val genderRatings = ratings.map(x => (x._1,(x._1,x._2,x._3)))
      .join(usersRDD.map(_.split("::")).map(x => (x(0),x(1))).cache())
    genderRatings.take(10).foreach(println)
    val maleFilteredRatings: RDD[(String, String, String)] = genderRatings.filter(x => x._2._2.equals(male))
      .map(x => x._2._1)

    val femaleFilteredRatings = genderRatings.filter(x => x._2._2.equals(female))
      .map(x => x._2._1)

    println("所有电影中男性最喜欢的TOP10：")
    maleFilteredRatings.map(x =>(x._2,(x._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(x =>(x._2._1.toDouble / x._2._2 , x._1))
      .sortByKey(false)
      .map(x =>(x._2,x._1))
      .take(10)
      .foreach(println)

    println("所有电影中女性最喜欢的TOP10：")
    femaleFilteredRatings.map(x =>(x._2,(x._3.toDouble,1)))
      .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      .map(x =>(x._2._1.toDouble / x._2._2 , x._1))
      .sortByKey(false)
      .map(x =>(x._2,x._1))
      .take(10)
      .foreach(println)

    val targetQQUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_._2.equals("18"))
    val targetTaoBaoUsers = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_._2.equals("25"))
    /**
     * 在 Spark中如何实现mapjoin呢?显然要借助于 Broadcast,
     * 把数据广播到Executor级别，让该 Executor 上的所有任务共享该唯一的数据，
     * 而不是每次运行 Task 的时候,都要发送一份数据的复制，这显著地降低了网络数据的传输和JVM内存的消耗
     */
    val targetQQUsersSet = HashSet() ++ targetQQUsers.map(_._1).collect()
    val targetTaoBaoUsersSet = HashSet() ++ targetTaoBaoUsers.map(_._1).collect()
    val targetQQUsersBroadcast = sc.broadcast(targetQQUsersSet)
    val targetTaoBaoUsersBroadcast = sc.broadcast(targetTaoBaoUsersSet)

    val movieID2Name = moviesRDD.map(_.split("::")).map(x =>(x(0),x(1))).collect.toMap
    println("所有电影中QQ或者微信核心目标用户最喜爱电影TopN分析：")
    ratingsRDD.map(_.split("::")).map(x => (x(0),x(1))).filter(x => targetQQUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1)).reduceByKey(_+_).map(x =>(x._2,x._1)).sortByKey(false).map(x => (x._2,x._1)).take(10)
      .map(x => (movieID2Name.getOrElse(x._1,null), x._2)).foreach(println)

    println("所有电影中淘宝核心目标用户最喜爱的电影TopN分析：")
    ratingsRDD.map(_.split("::")).map(x => (x(0),x(1))).filter(x => targetTaoBaoUsersBroadcast.value.contains(x._1))
      .map(x => (x._2, 1)).reduceByKey(_+_).map(x => (x._2,x._1))
      .sortByKey(false).map(x => (x._2,x._1)).take(10)
      .map(x => (movieID2Name.getOrElse(x._1,null), x._2)).foreach(println)

    println("对电影评分数据以Timestamp和Rating两个维度进行二次降序排列：")
    val pairWithSortkey = ratingsRDD.map(line =>{
      val splited =line.split("::")
      (new SecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)
    })
    val sorted = pairWithSortkey.sortByKey(false)
    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.take(10).foreach(println)
  }
  class SecondarySortKey(val first:Double,val second:Double) extends Ordered[SecondarySortKey] with Serializable{
    override def compare(that: SecondarySortKey): Int = {
      if (this.first - that.first != 0){
        (this.first - that.first).toInt
      }else{
        if(this.second - that.second > 0){
          Math.ceil(this.second - that.second).toInt
        }else if(this.second - that.second < 0){
          Math.floor(this.second - that.second).toInt
        }else{
          (this.second - that.second).toInt
        }
      }
    }
  }


}
