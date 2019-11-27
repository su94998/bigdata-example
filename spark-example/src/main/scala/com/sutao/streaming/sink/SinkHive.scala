package com.sutao.streaming.sink

import com.sutao.model.UserInfo1
import com.sutao.util.{GraceCloseUtils, MyKafkaUtil}
import net.sf.json.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.joda.time.LocalDate

/**
  * Created by sutao on 2019/11/15.
  */
object SinkHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SinkHive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // 开启优雅关闭
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val topic = "user1_log"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))


    kafkaDStream.foreachRDD(rdd => {

      if(!rdd.isEmpty()){

      // 获取 offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 业务逻辑处理
      val userInfoRDD=rdd.map(line=>{

        val json = JSONObject.fromObject(line.value)

        UserInfo1(json.get("name").toString,json.get("age").toString,json.get("record_time").toString)

      })

      import spark.implicits._

      userInfoRDD.toDS().createOrReplaceTempView("temp_userinfo")

      // 建立hive临时数据分区表，一天生成一个文件(既然是写入hive，一般就是批处理，比如T+1计算则第二天先根据记录时间动态分区再进行处理)
      spark.sql(
        s"""
           |insert into tmp_user_info partition(dt='${LocalDate.now()}')
           |select
           |name,
           |age,
           |record_time
           |from
           |temp_userinfo
         """.stripMargin)

      // 等输出操作完成后提交offset
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }})

    ssc.start()
    // 优雅关闭
    GraceCloseUtils.stopByMarkFile(ssc)
    ssc.awaitTermination()




  }

}
