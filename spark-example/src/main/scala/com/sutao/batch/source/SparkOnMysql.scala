package com.sutao.batch.source

import java.util.Properties

import com.sutao.util.PropertiesUtil
import org.apache.spark.sql.{SparkSession}

/**
  * Created by sutao on 2019/11/15.
  */
object SparkOnMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkOnHbase")
      .master("local[*]")
      .getOrCreate()

    // 不过滤读取
    val wordCountDF = readMysqlTable(spark, "word_count")
    wordCountDF.show()

    // 过滤读取
    val wordCountFilterDF = readMysqlTable(spark, "word_count", s"word='spark'")
    wordCountFilterDF.show()


  }


  /**
    * 获取 Mysql 表的数据
    *
    * @param spark
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(spark: SparkSession, tableName: String) = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    spark
      .read
      .format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", tableName)
      .load()

  }

  /**
    * 获取 Mysql 表的数据 添加过滤条件
    *
    * @param spark
    * @param table           读取Mysql表的名字
    * @param filterCondition 过滤条件
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(spark: SparkSession, table: String, filterCondition: String) = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    var tableName = ""
    tableName = "(select * from " + table + " where " + filterCondition + " ) as t1"
    spark
      .read
      .format("jdbc")
      .option("url", properties.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", properties.getProperty("jdbc.user"))
      .option("password", properties.getProperty("jdbc.password"))
      .option("dbtable", tableName)
      .load()
  }

}
