package com.tags

import com.util.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsAPP extends Tags {
  /**
   * 打app标签
   */
  override def makeTags(args: Any*): List[(String, Int)] = {
        //标签格式： APPxxxx->1）xxxx 为 App 名称，
        // 使用缓存文件 appname_dict 进行名称转换；APP 爱奇艺->1
        var list = List[(String, Int)]()
        val row: Row = args(0).asInstanceOf[Row]
        val dic = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
        //获取appid和appname
        val appId = row.getAs[String]("appid")
        val appName = row.getAs[String]("appname")
        if (StringUtils.isNoneBlank(appName)) {
          list :+= ("APP" + appName, 1)
        }else if(StringUtils.isNoneBlank(appId)) {
          list:+=("APP"+dic.value.getOrElse(appId,"其他"),1)
        }
    list
  }
}
