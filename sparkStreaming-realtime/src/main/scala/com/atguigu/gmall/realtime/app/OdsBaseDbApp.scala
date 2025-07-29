package com.atguigu.gmall.realtime.app

import java.util
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._


/**
 * 业务数据分流
 * 1. 读取偏移量
 * 2. 接收 Kafka 数据
 * 3. 提取偏移量结束点
 * 4. 转换结构
 * 5. 分流处理
 *    5.1 事实数据分流 -> Kafka
 *    5.2 维度数据分流 -> Redis
 * 6. 提交偏移量
 */
object OdsBaseDbApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("base_db_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_BASE_DB"
    val groupId = "ods_base_db_group"

    // 1. 读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topic, groupId)

    // 2. 接收 Kafka 数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, offsets, groupId)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, groupId)
    }

    // 3. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = Array.empty
    val transformedDStream = kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    // 4. 转换结构：String -> JSONObject
    val jsonObjDStream: DStream[JSONObject] = transformedDStream.map { record =>
      val jsonStr = record.value()
      JSON.parseObject(jsonStr)
    }

    // 5. 分流处理
    jsonObjDStream.foreachRDD { rdd =>
      val jedis = MyRedisUtils.getJedisClient
      // 读取 Redis 中维度表和事实表清单
      val dimTables: util.Set[String] = jedis.smembers("DIM:TABLES")
      val factTables: util.Set[String] = jedis.smembers("FACT:TABLES")

      jedis.close()

      val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
      val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

      rdd.foreachPartition { jsonIter =>
        val jedis = MyRedisUtils.getJedisClient

        for (jsonObj <- jsonIter) {
          val tableName = jsonObj.getString("table")
          val optType = jsonObj.getString("type")

          val opt: String = optType match {
            case "bootstrap-insert" | "insert" => "I"
            case "update"                      => "U"
            case "delete"                      => "D"
            case _                             => null
          }

          if (opt != null) {
            val dataJson = jsonObj.getJSONObject("data")
            if (dataJson != null) {
              val dataJsonArray: JSONObject =jsonObj.getJSONObject("data")


              // 5.1 事实数据分流 -> Kafka
              if (factTablesBC.value.contains(tableName)) {
                val topicName = s"DWD_${tableName.toUpperCase}_$opt"
                val key: String = dataJsonArray.getString("id")
                MyKafkaUtils.send(topicName, key, dataJson.toJSONString)
              }

              // 5.2 维度数据分流 -> Redis
              if (dimTablesBC.value.contains(tableName)) {
                val id = dataJson.getString("id")
                val redisKey : String =s"DIM:${tableName.toUpperCase()}:$id"
                jedis.set(redisKey, dataJson.toJSONString)
              }
            }
          }
        }
        jedis.close()
        MyKafkaUtils.flush()
      }
      // 6. 提交偏移量
      MyOffsetsUtils.saveOffset(topic, groupId, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
