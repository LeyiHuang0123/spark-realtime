package com.atguigu.gmall.realtime.util

/**
 * 配置 Key 常量池
 *
 * 说明：
 *   - 这里只放「键名」常量，真正的取值仍然来自
 *     src/main/resources/config.properties 或环境变量。
 *   - 在其他代码里这样用：
 *       val servers = MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS)
 */
object MyConfig {

  /** Kafka */
  val KAFKA_BOOTSTRAP_SERVERS: String = "kafka.bootstrap.servers"
  val KAFKA_DEFAULT_GROUP_ID: String  = "kafka.default.group.id"

  /** Redis */
  val REDIS_HOST: String = "redis.host"
  val REDIS_PORT: String = "redis.port"

  /** Spark Streaming */
  val SPARK_BATCH_INTERVAL: String = "spark.batch.interval"

  /** ElasticSearch */
  val ES_HOSTS: String = "es.hosts"
}
