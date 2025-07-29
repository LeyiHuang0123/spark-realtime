package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtils {

  private var jedisPool: JedisPool = _

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      synchronized {
        if (jedisPool == null) {
          val host: String = MyPropsUtils(MyConfig.REDIS_HOST)
          val port: String = MyPropsUtils(MyConfig.REDIS_PORT)

          val poolConfig = new JedisPoolConfig()
          poolConfig.setMaxTotal(100)
          poolConfig.setMaxIdle(20)
          poolConfig.setMinIdle(20)
          poolConfig.setBlockWhenExhausted(true)
          poolConfig.setMaxWaitMillis(5000)
          poolConfig.setTestOnBorrow(true)

          jedisPool = new JedisPool(poolConfig, host, port.toInt)
        }
      }
    }
    jedisPool.getResource
  }

  def close(jedis: Jedis): Unit = {
    if (jedis != null) jedis.close()
  }
}
