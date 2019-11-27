package com.sutao.streaming.offset

import com.sutao.util.{GraceCloseUtils, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sutao on 2019/10/16.
  *
  * 0.10版本kafka提供的api保存offset
  *
  * Spark Streaming优雅关闭服务策略和冷启动时Kafka数据堆积优化
  *
  */
object KafkaOffsetManager {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaOffsetManager").setMaster("local[2]")

    /* 启动优雅关闭服务 */
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")

    /* Spark Streaming 重启后Kafka数据堆积调优 */
    sparkConf.set("spark.streaming.backpressure.enabled", "true") // 激活反压功能
    sparkConf.set("spark.streaming.backpressure.initialRate", "5000") // 启动反压功能后，读取的最大数据量
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "2000") // 设置每秒每个分区最大获取日志数，控制处理数据量，保证数据均匀处理。

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))

    kafkaDStream.foreachRDD(rdd => {
      // 只处理有数据的rdd，没有数据的直接跳过
      if(!rdd.isEmpty()){

      // 获取rdd offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 业务逻辑处理
      // 迭代分区，里面的代码是运行在executor上面
      rdd.foreachPartition(items => {
        items.foreach(println)
      })

      // 等输出操作完成后提交offset
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }})


    // 启动
    ssc.start()

    // 方式一： 通过Http方式优雅的关闭策略
    // GraceCloseUtils.daemonHttpServer(8012,ssc)
    // 方式二： 通过扫描HDFS文件来优雅的关闭
    GraceCloseUtils.stopByMarkFile(ssc)

    // 线程阻塞
    ssc.awaitTermination()


  }

}
