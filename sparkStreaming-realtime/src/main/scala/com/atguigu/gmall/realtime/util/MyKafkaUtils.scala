package com.atguigu.gmall.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * Kafka 工具类：既能消费也能生产
 */
object MyKafkaUtils {

  /* ---------------- 公共配置 ---------------- */
  private val BOOTSTRAP_SERVERS: String = MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS)
  private val DEFAULT_GROUP_ID:  String = "spark-realtime"

  /* ---------------- 消费端参数 ---------------- */
  private val consumerConfig: mutable.Map[String, Object] = mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG   -> BOOTSTRAP_SERVERS,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG   -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG            -> DEFAULT_GROUP_ID,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG   -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG  -> "true"
  )

  /* ====================== 1. 消费 ====================== */

  /** 默认（latest 或 earliest）offset 消费 */
  def getKafkaDStream(topic: String,
                      ssc: StreamingContext,
                      groupId: String = DEFAULT_GROUP_ID
                     ): InputDStream[ConsumerRecord[String, String]] = {

    consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig.toMap)
    )
  }

  /** 指定 offset 消费 */
  def getKafkaDStream(topic:    String,
                      ssc:      StreamingContext,
                      offsets:  Map[TopicPartition, Long],
                      groupId:  String
                     ): InputDStream[ConsumerRecord[String, String]] = {

    consumerConfig(ConsumerConfig.GROUP_ID_CONFIG) = groupId

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfig.toMap, offsets)
    )
  }

  /* ====================== 2. 生产 ====================== */

  /** 创建 Producer 单例（幂等性开启） */
  @transient private lazy val producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")   // 幂等
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    new KafkaProducer[String, String](props)
  }

  /** 便捷发送（无 key） */
  def send(topic: String, msg: String): Unit =
    producer.send(new ProducerRecord[String, String](topic, msg))

  /** 便捷发送（带 key） */
  def send(topic: String, key: String, msg: String): Unit =
    producer.send(new ProducerRecord[String, String](topic, key, msg))

  /** 刷写缓冲区 */
  def flush(): Unit = if (producer != null) producer.flush()

  /** 关闭 Producer（应用退出时调用） */
  def close(): Unit = if (producer != null) producer.close()
}
