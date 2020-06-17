package com.test

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EquipmentRpt {
  def main(args: Array[String]): Unit = {

    // 创建执行入口
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    //读取数据
    val df: DataFrame = spark.read.parquet("D:\\TestLog")
    //数据处理
    //注册临时视图
    df.createTempView("log")

    val df3 = spark.sql(
      """
        |select
        |client,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end) originalRequest,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end) effectiveRequest,
        |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) participateInBid,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end) bidSuccessfully,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end) shows,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0 end) dspcost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0 end) adcost
        |from log
        |group by client
        |""".stripMargin)
    //将结果写入MySQL数据库
    //加载配置文件（json，conf,properties）
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    df3.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(
      load.getString("jdbc.url"),
      load.getString("jdbc.TabName1"),
      prop

    )
    //关闭
    spark.stop()
  }
}
