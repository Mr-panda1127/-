package com.tags

import com.util.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 上下文标签
 */
object TagsContext {
  def main(args: Array[String]): Unit = {

    val Array(inputPath,app_dic,stopWords)= args
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    //处理字典
    val dicMap = spark.sparkContext.textFile(app_dic).map(_.split("\t",-1)).filter(_.length>=5)
      .map(arr=>{
        (arr(4),arr(1))
      }).collectAsMap()

    //广播字典
    val mapBroad = spark.sparkContext.broadcast(dicMap)

    // 读取停用词库
    val arr = spark.sparkContext.textFile(stopWords).collect()
    val stopBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)

    //  获取数据
    val df: DataFrame = spark.read.parquet(inputPath)

    //判断用户的唯一ID必须要存在
    df.filter(TagUtils.OneUserId)

      //进行打标签处理
      .rdd.map(row=>{

      // 获取不为空的唯一UserId
      val userId = TagUtils.getAnyOneUserId(row)

      // 广告类型标签(渠道标签，地域标签都在里面)
      val adList: List[(String, Int)] = TagsAD.makeTags(row)

      //APP标签
      val appList = TagsAPP.makeTags(row, mapBroad)

      //设备标签
      val devList = TagsDev.makeTags(row)

      // 关键字标签
      val kwList = TagsKeyWord.makeTags(row, stopBroad)

      //要打哪个标签就返回哪个k
//      (userId,adList)
    }).foreach(println)
  }
}
