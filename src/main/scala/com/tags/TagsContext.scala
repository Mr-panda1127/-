package com.tags

import com.typesafe.config.ConfigFactory
import com.util.{TagUtils,JedisConnection}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 上下文标签
 */
object TagsContext {
  def main(args: Array[String]): Unit = {

    val Array(inputPath,app_dic,stopWords,day)= args
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    /*
    整合Hbase
     */
    val load = ConfigFactory.load()
    val hbaseTabName = load.getString("hbase.TabName")

    // 加载配置
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zk"))

    // 创建任务，并获取Connection连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val admin = hbconn.getAdmin

    // 如果当表不存在，那么需要创建一个新表，存储则反之
    if (!admin.tableExists(TableName.valueOf(hbaseTabName))) {
      println("————创建表！！————")
      // 创建对象
      val tableNameDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTabName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加载到表中
      tableNameDescriptor.addFamily(columnDescriptor)
      // 创建表
      admin.createTable(tableNameDescriptor)
      // 关闭
      admin.close()
      hbconn.close()
    }


    // 创建JobConf
    val jobConf = new JobConf(configuration)
    // 指定key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTabName)


    // 处理字典
    val dicMap = spark.sparkContext.textFile(app_dic).map(_.split("\t",-1)).filter(_.length>=5)
      .map(arr=>{
        (arr(4),arr(1))
      }).collectAsMap()


    // 广播字典
    val mapBroad = spark.sparkContext.broadcast(dicMap)


    // 读取停用词库
    val arr = spark.sparkContext.textFile(stopWords).collect()
    val stopBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)

    // 获取数据
    val df: DataFrame = spark.read.parquet(inputPath)
    // 判断用户的唯一ID必须要存在
    df.filter(TagUtils.OneUserId)
      // 进行打标签处理
      .rdd.mapPartitions(rdd=>{
      //      val jedis = JedisConnectionPool.getConnection()
      val ite: Iterator[(String, List[(String, Int)])] = rdd.map(row => {
        // 获取不为空的唯一UserId
        val userId = TagUtils.getAnyOneUserId(row)
        // 获取用户所有不为空的ID

        // 广告类型标签
        val adList: List[(String, Int)] = TagsAD.makeTags(row)
        // APP标签
        val appList = TagsAPP.makeTags(row, mapBroad)
        // 设备标签
        val devList = TagsDev.makeTags(row)
        // 关键字标签
        val kwList = TagsKeyWord.makeTags(row, stopBroad)
        // 商圈标签
        //         val busList = TagsBusiness.makeTags(row, jedis)
        (userId, adList++appList++devList++kwList)
      })
      //      jedis.close()
      ite
    }).reduceByKey((list1,list2)=>{
      val list: List[((String, Int), (String, Int))] = list1.zip(list2)
      list.map(t=>(t._1._1,t._2._2+t._1._2))
    }).map{
      case (userId,userTags)=>{
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),
          Bytes.toBytes(day),
          Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
      // 存入HBASE
    }.saveAsHadoopDataset(jobConf)
  }
}
