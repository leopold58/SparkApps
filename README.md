# SparkApps开发笔记

> 电影推荐系统( MovieLens)的数据下载地址为: https://grouplens.org/datasets/movielens/。 GroupLens 项目研究收集了从电影推荐系统 MovieLens 站点提供评级的数据集 (http://MovieLens.org) ， 收集了不同时间段的数据，我们可以根据电影分析业务需求下载不同规模大小的数据源文件。

配置环境和依赖-安装maven环境，配置好pom文件



```
* rating.dat的数据格式
* UserID::MovieID::Rating::Timestamp
* 用户 ID、电影 ID、评分数据、时间戳
```

* rating.dat中读取电影的评分数据,计算电影的平均评分，并获得平均评分较高的10个电影；
* 读取电影的观看人次，将电影的观看人次进行聚合，得到电影的观看人数并排序，获得观看人数最多的10个电影。

