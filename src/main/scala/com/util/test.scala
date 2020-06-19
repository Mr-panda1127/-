package com.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      val spark = SparkSession.builder()
        .appName("log2Parquet")
        .master("local")
        .config(conf) // 加载配置
        .getOrCreate()
      val value = spark.sparkContext.parallelize(Array(("user1", List(("爱奇艺", 1), ("优酷", 1), ("腾讯", 1), ("爱奇艺", 1))), ("user1", List(("爱奇艺", 1), ("优酷", 1), ("腾讯", 1)))))
        .reduceByKey((list1, list2) => {
          (list1 ::: list2).groupBy(_._1)
            .mapValues(_.foldLeft(0)(_+_._2))
            .toList
        }).foreach(println)

    }

}
