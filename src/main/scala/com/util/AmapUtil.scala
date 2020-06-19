package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

/**
 * 从高德获取商圈信息
 */
object AmapUtil {

  def getBusinessFromAmap(long:Double,lat:Double):String={
    // https://restapi.amap.com/v3/geocode/regeo
    // ?location=116.310003,39.991957&key=<用户的key>&radius=3000
    // 拼接URL
    val location = long+","+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=&radius=3000"
    // 发送HTTP请求协议
    val json = HttpUtilTest.get(url)
    /*
    {
  "status": "1",
  "regeocode": {
    "addressComponent": {
      "city": [],
      "province": "北京市",
      "adcode": "110105",
      "district": "朝阳区",
      "towncode": "110105026000",
      "streetNumber": {
        "number": "6号",
        "location": "116.482005,39.9900561",
        "direction": "东南",
        "distance": "63.2126",
        "street": "阜通东大街"
      },
      "country": "中国",
      "township": "望京街道",
      "businessAreas": [
        {
          "location": "116.470293,39.996171",
          "name": "望京",
          "id": "110105"
        },
        {
          "location": "116.494356,39.971563",
          "name": "酒仙桥",
          "id": "110105"
        },
        {
          "location": "116.492891,39.981321",
          "name": "大山子",
          "id": "110105"
        }
      ],
      "building": {
        "name": "方恒国际中心B座",
        "type": "商务住宅;楼宇;商务写字楼"
      },
      "neighborhood": {
        "name": "方恒国际中心",
        "type": "商务住宅;楼宇;商住两用楼宇"
      },
      "citycode": "010"
    },
    "formatted_address": "北京市朝阳区望京街道方恒国际中心B座"
  },
  "info": "OK",
  "infocode": "10000"
}
     */
    //    println(json)
    var list = ListBuffer[String]()
    val jObject = JSON.parseObject(json)
    // 判断当前的json串必须有数据
    val status = jObject.getIntValue("status")
    if(status == 0) return ""
    // 如果不为空，再次获取值
    val regeocode = jObject.getJSONObject("regeocode")
    if(regeocode == null) return ""
    val address = regeocode.getJSONObject("addressComponent")
    if(address == null) return ""
    val businessAreas = address.getJSONArray("businessAreas")
    if(businessAreas ==null) return ""

    // 循环处理数组
    for(arr<-businessAreas.toArray){
      // 将元素类型转换JSON
      if(arr.isInstanceOf[JSONObject]){
        val json = arr.asInstanceOf[JSONObject]
        val name = json.getString("name") // 商圈名称
        list.append(name)
      }
    }
    list.mkString(",")
  }
}
