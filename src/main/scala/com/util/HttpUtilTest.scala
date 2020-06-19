package com.util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
 * 发生GET请求
 */
object HttpUtilTest {
  def get(url:String):String={
    val client = HttpClients.createDefault()
    val hGet = new HttpGet(url)
    //发送请求
    val response = client.execute(hGet)
    // 获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
