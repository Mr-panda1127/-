package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
object TagUtils {
  // 获取用户唯一不为空的ID
  def getAnyOneUserId(row: Row) ={
    row match {
      case v:Row if StringUtils.isNotBlank(v.getAs[String]("imei"))
      => "TM:"+v.getAs[String]("imei")
      case v:Row if StringUtils.isNotBlank(v.getAs[String]("mac"))
      => "MC:"+v.getAs[String]("mac")
      case v:Row if StringUtils.isNotBlank(v.getAs[String]("idfa"))
      => "ID:"+v.getAs[String]("idfa")
      case v:Row if StringUtils.isNotBlank(v.getAs[String]("openudid"))
      => "OD:"+v.getAs[String]("openudid")
      case v:Row if StringUtils.isNotBlank(v.getAs[String]("androidid"))
      => "AD:"+v.getAs[String]("androidid")
    }
  }

  val OneUserId =
    """
      |imei !='' or mac !='' or idfa!= '' or openudid!='' or androidid !=''
      |""".stripMargin
}
