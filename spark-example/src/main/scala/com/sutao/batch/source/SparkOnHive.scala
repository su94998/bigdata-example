package com.sutao.batch.source

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sutao on 2019/11/15.
  *
  */
object SparkOnHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkOnHive")
      .master("local[*]")
      .config(new SparkConf().set("spark.default.parallelism","18")) // 指定rdd发生shuffle时的并行度
      .enableHiveSupport()
      .getOrCreate()

    // 指定数据库
   // spark.sql("use ods_db")

    // 开启动态分区
   // spark.sql("set hive.exec.dynamic.partition=true")
   // spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    // 注册自定义函数
   // spark.udf.register("",)

    // 设置spark sql 发生shuffle时的分区数即并行度，不指定时发生shuffle的默认并行度是200
   // spark.conf.set("spark.sql.shuffle.partitions","100")

    // 查询
    spark.sql(
      s"""
         |select
         |*
         |from test_table
       """.stripMargin).show(100)
    
  }

}
