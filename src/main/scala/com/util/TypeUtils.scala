package com.util

/**
 * 类型转换工具类
 */
object TypeUtils {
  // 转换Int类型
  def str2Int(str:String):Int={
    try {
      str.toInt
    }catch {
      case _:Exception=> 0
    }
  }
  // 转换Int类型
  def str2Double(str:String):Double={
    try {
      str.toDouble
    }catch {
      case _:Exception=> 0.0
    }
  }
}
