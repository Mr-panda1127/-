package com.test

import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 媒体维度统计
 */
object APPRpt {
  def main(args: Array[String]): Unit = {
    if (args.length !=2) {
      sys.exit()
    }
    val Array(inputPath,app_dic)=args
    //创建执行入口
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    //处理字典
    val dicMap = spark.sparkContext.textFile(app_dic).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collectAsMap()

    //广播字典
    val mapBroad = spark.sparkContext.broadcast(dicMap)

    //获取数据
    val df = spark.read.parquet(inputPath)
    df.rdd.map((row:Row)=> {
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")

      //判断AppName是否存在
      if (StringUtils.isBlank(appname)) {
          appname = mapBroad.value.getOrElse(appid,"其他")
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("isbid")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment =  row.getAs[Double]("adpayment")
      val list1 = RptUtils.requestProcessor(requestmode, processnode)
      val list2 = RptUtils.isBidAndWin(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val list3 = RptUtils.showAndClk(requestmode, iseffective)
      (appname,list1++list2++list3)
    })

    //聚合
      .reduceByKey((list1,list2)=>{
        list1.zip(list2)
          .map(t=>t._1+t._2)
      }).foreach(println)

    //关闭
    spark.stop()
  }
}
