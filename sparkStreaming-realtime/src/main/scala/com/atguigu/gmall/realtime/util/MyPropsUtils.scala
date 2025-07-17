package com.atguigu.gmall.realtime.util

import org.codehaus.janino.Java.TryStatement.Resource

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String): String =
    if (bundle.containsKey(propsKey)) bundle.getString(propsKey).trim
    else throw new NoSuchElementException(s"Config key not found: $propsKey")

  /** 辅助：转 Int */
  def getInt(propsKey: String): Int = apply(propsKey).toInt

  /** 辅助：转 Long */
  def getLong(propsKey: String): Long = apply(propsKey).toLong

  /** 辅助：转 Boolean */
  def getBoolean(propsKey: String): Boolean = apply(propsKey).toBoolean

  /** -------- 临时 main：直接运行文件自测 -------- */
  def main(args: Array[String]): Unit = {
    val bootstrap = apply("kafka.bootstrap.servers")
    println(s"kafka.bootstrap.servers = $bootstrap")
  }
}
