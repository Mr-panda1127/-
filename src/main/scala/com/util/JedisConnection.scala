package com.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


/**
 * 获取连接
 */
object JedisConnection {

  private val config = new GenericObjectPoolConfig
  config.setMaxTotal(10)
  config.setMaxIdle(5)

  private val pool = new JedisPool(config, "hadoop11", 6379, 10000, "lyl19951127")
  def getConnection() ={
    pool.getResource
  }
}
