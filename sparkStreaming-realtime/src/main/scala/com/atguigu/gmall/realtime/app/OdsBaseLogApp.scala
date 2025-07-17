package com.atguigu.gmall.realtime.app


import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{StartLog, PageLog, PageDisplayLog, PageActionLog}
import com.atguigu.gmall.realtime.util.{MyConfig, MyKafkaUtils, MyPropsUtils}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream

import scala.util.Try
import scala.collection.JavaConverters._

/**
 * 日志数据消费分流：
 *   1. 准备 StreamingContext
 *   2. Kafka -> ODS_BASE_LOG
 *   3. 处理数据
 *    3.1 转换数据结构 JSON 解析
 *    3.2 分流（err/start/page/display/action）
 *      日志数据
 *        页面访问数据（公共字段/页面数据/曝光数据/事件数据/错误数据）
 *        启动数据（公共字段/启动数据/错误数据）
 *   4. 写出到 DWD 各 Topic
 *
 * 需要的配置（resources/config.properties）:
 *   kafka.bootstrap.servers=localhost:9092
 *   spark.batch.interval=5
 */
object OdsBaseLogApp {

  private val SOURCE_TOPIC     = "ODS_BASE_LOG"
  private val DWD_ERROR_LOG    = "DWD_ERROR_LOG"
  private val DWD_START_LOG    = "DWD_START_LOG"
  private val DWD_PAGE_LOG     = "DWD_PAGE_LOG"
  private val DWD_DISPLAY_LOG  = "DWD_DISPLAY_LOG"
  private val DWD_ACTION_LOG   = "DWD_ACTION_LOG"
  private val fastjsonCfg      = new SerializeConfig(true)
  private val fastjsonFeatures = Array(SerializerFeature.DisableCircularReferenceDetect)

  def main(args: Array[String]): Unit = {

    // ---------------- 1. StreamingContext ----------------
    val batchSec: Int = Try(MyPropsUtils.getInt(MyConfig.SPARK_BATCH_INTERVAL)).getOrElse(5)

    val conf = new SparkConf()
      .setAppName("ods_base_log_app")
      .setMaster("local[4]") // 本地调试；上线请改集群 master
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(batchSec))

    // ---------------- 2. Kafka 源流 ----------------
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtils.getKafkaDStream(SOURCE_TOPIC, ssc, "ods_base_log_group")

    // ---------------- 3. 处理逻辑：分流 ----------------
    kafkaDStream.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        iter.foreach { record =>
          val jsonStr = record.value()
          try {
            val obj: JSONObject = JSON.parseObject(jsonStr)
            val common: JSONObject = obj.getJSONObject("common")
            val ts: Long           = obj.getLongValue("ts") // 根时间戳
            // === helper 函数 ===
            @inline def gs(o: JSONObject, k: String): String =
              if (o != null && o.containsKey(k)) Option(o.getString(k)).getOrElse("") else ""
            @inline def gl(o: JSONObject, k: String): Long =
              if (o != null && o.containsKey(k) && o.get(k) != null) o.getLongValue(k) else 0L

            // ---------- 3.1 错误日志 ----------
            val errObj: JSONObject = obj.getJSONObject("err")
            if (errObj != null) {
              val errorJson = new JSONObject()
              if (common != null) errorJson.put("common", common)  // 携带公共维度
              errorJson.put("ts", ts)
              errorJson.put("err", errObj)                         // 保留完整 err 对象（含 msg、error_code）
              MyKafkaUtils.send(DWD_ERROR_LOG, errorJson.toJSONString)
              obj.remove("err") // 去掉避免后面重复处理
            }

            // ---------- 3.2 启动日志 ----------
            val startObj: JSONObject = obj.getJSONObject("start")
            if (startObj != null) {
              // 扁平化 start -> StartLog
              val startLog = StartLog(
                mid              = gs(common, "mid"),
                user_id          = gs(common, "uid"),
                province_id      = gs(common, "ar"),
                channel          = gs(common, "ch"),
                is_new           = gs(common, "is_new"),
                model            = gs(common, "md"),
                operate_system   = gs(common, "os"),
                version_code     = gs(common, "vc"),
                brand            = gs(common, "ba"),
                entry            = gs(startObj, "entry"),
                loading_time     = gl(startObj, "loading_time"),
                open_ad_id       = gs(startObj, "open_ad_id"),
                open_ad_ms       = gl(startObj, "open_ad_ms"),
                open_ad_skip_ms  = gl(startObj, "open_ad_skip_ms"),
                ts               = ts
              )
              MyKafkaUtils.send(
                DWD_START_LOG,
                JSON.toJSONString(startLog, fastjsonCfg, fastjsonFeatures:_*)
              )
            } else {
              // ---------- 3.3 页面及子日志 ----------
              // ---------- 提取公共字段 ----------
              // common字段
              val mid    = gs(common, "mid")
              val uid    = gs(common, "uid")
              val ar     = gs(common, "ar")
              val ch     = gs(common, "ch")
              val isNew  = gs(common, "is_new")
              val md     = gs(common, "md")
              val os     = gs(common, "os")
              val vc     = gs(common, "vc")
              val ba     = gs(common, "ba")

              // 页面对象
              val pageObj: JSONObject = obj.getJSONObject("page")
              if (pageObj != null) {
                // 兼容 page_id / id
                val pageId = {
                  val v = gs(pageObj, "page_id")
                  if (v.nonEmpty) v else gs(pageObj, "id")
                }
                val lastPageId  = gs(pageObj, "last_page_id")
                val pageItem    = gs(pageObj, "item")
                val pageItemTyp = gs(pageObj, "item_type")
                val duringTime  = gl(pageObj, "during_time")
                val sourceType  = gs(pageObj, "sourceType")

                // 封装 PageLog
                val pageLog = PageLog(
                  mid, uid, ar, ch, isNew, md, os, vc, ba,
                  pageId, lastPageId, pageItem, pageItemTyp,
                  duringTime, sourceType, ts
                )

                // 写 Page 主流（序列化成 JSON）
                MyKafkaUtils.send(DWD_PAGE_LOG, JSON.toJSONString(pageLog, fastjsonCfg, fastjsonFeatures:_*)
                )

                // ---------- 曝光 displays ----------
                val displays: JSONArray = obj.getJSONArray("displays")
                if (displays != null && !displays.isEmpty) {
                  displays.asScala.foreach { d =>
                    val dObj = d.asInstanceOf[JSONObject]

                    val displayType     = gs(dObj, "display_type")
                    val displayItem     = gs(dObj, "item")
                    val displayItemType = gs(dObj, "item_type")
                    val posId           = gs(dObj, "pos_id")
                    val order           = gs(dObj, "order")

                    val dispLog = PageDisplayLog(
                      mid               = mid,
                      user_id           = uid,
                      province_id       = ar,
                      channel           = ch,
                      is_new            = isNew,
                      model             = md,
                      operate_system    = os,
                      version_code      = vc,
                      brand             = ba,
                      page_id           = pageId,
                      last_page_id      = lastPageId,
                      page_item         = pageItemTyp,
                      page_item_type    = pageItemTyp,
                      during_time       = duringTime,
                      sourceType        = sourceType,
                      display_type      = displayType,
                      display_item      = displayItem,
                      display_item_type = displayItemType,
                      display_order     = order,
                      display_pos_id    = posId,
                      ts                = ts
                    )

                    MyKafkaUtils.send(
                      DWD_DISPLAY_LOG,
                      JSON.toJSONString(dispLog, fastjsonCfg, fastjsonFeatures:_*)
                    )
                  }
                }


                // ---------- 动作 actions ----------
                val actions: JSONArray = obj.getJSONArray("actions")
                if (actions != null && !actions.isEmpty) {
                  actions.asScala.foreach { a =>
                    val aObj = a.asInstanceOf[JSONObject]
                    val actLog = PageActionLog(
                      mid              = mid,
                      user_id          = uid,
                      province_id      = ar,
                      channel          = ch,
                      is_new           = isNew,
                      model            = md,
                      operate_system   = os,
                      version_code     = vc,
                      brand            = ba,
                      page_id          = pageId,
                      action_id        = gs(aObj, "action_id"),
                      action_item      = gs(aObj, "item"),
                      action_item_type = gs(aObj, "item_type"),
                      action_ts        = gl(aObj, "ts"),
                      ts               = ts
                    )
                    MyKafkaUtils.send(
                      DWD_ACTION_LOG,
                      JSON.toJSONString(actLog, fastjsonCfg, fastjsonFeatures:_*)
                    )
                  }
                }
              }
            }
          } catch {
            case e: Exception =>
              val errorJson = new JSONObject()
              errorJson.put("bad_record", jsonStr)
              errorJson.put("err_msg", Option(e.getMessage).getOrElse(e.getClass.getName))
              MyKafkaUtils.send(DWD_ERROR_LOG, errorJson.toJSONString)
          }
        }
        MyKafkaUtils.flush()
      }
    }
    // ---------------- 4. 启动 ----------------
    ssc.start()
    ssc.awaitTermination()
  }
}
