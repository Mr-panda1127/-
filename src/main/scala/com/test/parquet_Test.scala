package com.test

import org.apache.spark.SparkConf

import org.apache.spark.sql.{DataFrame, SparkSession}

object parquet_Test {
  def main(args: Array[String]): Unit = {
//     参数判断
//        if(args.length !=2){
//          println("目录不正确，退出程序")
//          sys.exit()
//        }
    // 创建执行入口
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    //读取数据
    val df: DataFrame = spark.read.parquet("D:\\TestLog")
    //数据处理
    //注册临时视图
    df.createTempView("tempLog")
    //执行SQL语句
    val df2 = spark.sql(
      """
        |select
        |provincename,
        |cityname,
        |count(*) ct
        |from tempLog
        |group by provincename,cityname
        |""".stripMargin)

    df2.coalesce(1).write.json("output")
    //关闭
    spark.stop()
  }
}
