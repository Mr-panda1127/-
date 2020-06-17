package com.etl

import com.util.TypeUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * HDFS数据ETL处理缺省值字段，不符合要求字段
 */
object log2Parquet {
  def main(args: Array[String]): Unit = {
    // 参数判断
    //    if(args.length !=2){
    //      println("目录不正确，退出程序")
    //      sys.exit()
    //    }
    val Array(inputPath,outputPath)=args
    // 创建执行入口
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()
    // 读取数据
    //    val df = spark.read.text("data\\textLog.log")
    //    df.show(false)
    val lines = spark.sparkContext.textFile("data\\textLog.log")
    // 过滤数据，在切分的时候，如果有字符相连或者相连过长，程序使用split切分的时候，会默认把他当成一个字符处理
    // 那这样导致数据不准确，同时过滤的数据太多，所以我们切分的时候，最好在split中加上总的字符串长度即可（-1）
    val words = lines.filter(t=>t.split(",",t.length).length >= 85).map(t=>{
      val arr = t.split(",", -1)
      new log(
        arr(0),
        TypeUtils.str2Int(arr(1)) ,
        TypeUtils.str2Int(arr(2)),
        TypeUtils.str2Int(arr(3)),
        TypeUtils.str2Int(arr(4)),
        arr(5),
        arr(6),
        TypeUtils.str2Int(arr(7)),
        TypeUtils.str2Int(arr(8)),
        TypeUtils.str2Double(arr(9)),
        TypeUtils.str2Double(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        TypeUtils.str2Int(arr(17)),
        arr(18),
        arr(19),
        TypeUtils.str2Int(arr(20)),
        TypeUtils.str2Int(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        TypeUtils.str2Int(arr(26)),
        arr(27),
        TypeUtils.str2Int(arr(28)),
        arr(29),
        TypeUtils.str2Int(arr(30)),
        TypeUtils.str2Int(arr(31)),
        TypeUtils.str2Int(arr(32)),
        arr(33),
        TypeUtils.str2Int(arr(34)),
        TypeUtils.str2Int(arr(35)),
        TypeUtils.str2Int(arr(36)),
        arr(37),
        TypeUtils.str2Int(arr(38)),
        TypeUtils.str2Int(arr(39)),
        TypeUtils.str2Double(arr(40)),
        TypeUtils.str2Double(arr(41)),
        TypeUtils.str2Int(arr(42)),
        arr(43),
        TypeUtils.str2Double(arr(44)),
        TypeUtils.str2Double(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        TypeUtils.str2Int(arr(57)),
        TypeUtils.str2Double(arr(58)),
        TypeUtils.str2Int(arr(59)),
        TypeUtils.str2Int(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        TypeUtils.str2Int(arr(73)),
        TypeUtils.str2Double(arr(74)),
        TypeUtils.str2Double(arr(75)),
        TypeUtils.str2Double(arr(76)),
        TypeUtils.str2Double(arr(77)),
        TypeUtils.str2Double(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        TypeUtils.str2Int(arr(84))
      )
    })
    import spark.implicits._
    // 如果我们使用的是类，而不是样例类，那么此时类最多只能使用22个参数字段
    // 那么想要用类使用超过22个字段，需要继承product特质
    val df: DataFrame = words.toDF()
    // 将处理后的数据存入到存储系统（本地）
    df.write.parquet("D:\\TestLog")
        df.show()
  }
}