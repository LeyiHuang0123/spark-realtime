package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

object MyOffsetsUtils {

  /**
   * 从 Redis 中读取偏移量
   *
   * @param topic   Kafka 主题
   * @param groupId 消费组 ID
   * @return Map[TopicPartition, Long]
   */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedisClient
    val redisKey = s"offset:$topic:$groupId"
    val offsets = jedis.hgetAll(redisKey)
    println("读取到offset：" + offsets)

    val offsetMap: Map[TopicPartition, Long] = if (offsets != null && !offsets.isEmpty) {
      offsets.asScala.map {
        case (partition, offset) =>
          new TopicPartition(topic, partition.toInt) -> offset.toLong
      }.toMap
    } else {
      Map.empty[TopicPartition, Long]
    }

    jedis.close()
    offsetMap
  }

  /**
   * 将偏移量写入 Redis
   *
   * @param topic        Kafka 主题
   * @param groupId      消费组 ID
   * @param offsetRanges 本批次的偏移量范围
   */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.nonEmpty) {
      val offsets = new java.util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition = offsetRange.partition
        val endOffset = offsetRange.untilOffset
        offsets.put(partition.toString, endOffset.toString)
      }
      println("提交offset：" + offsets)

      val jedis = MyRedisUtils.getJedisClient // getJedisFromPool 改名后的版本
      val redisKey = s"offset:$topic:$groupId" // 和 readOffset 保持一致
      jedis.hmset(redisKey, offsets)
      jedis.close()
    }
  }
}

