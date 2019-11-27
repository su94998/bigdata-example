package com.sutao.streaming.sink

import java.util.Properties

import com.sutao.util.{KafkaSink, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

/**
  * Created by sutao on 2019/10/16.
  *
  * 消费kafka数据反写kafka
  *
  * 将kafkaProducer广播出去，提高性能
  *
  */
object SinkKafka {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SinkKafka").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log1"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))


    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "hadoop151:9092,hadoop152:9092,hadoop153:9092")
        properties.put("acks", "all")
        properties.put("retries ", "1")
        properties.setProperty("key.serializer", classOf[StringSerializer].getName)
        properties.setProperty("value.serializer", classOf[StringSerializer].getName)
        properties
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }


    kafkaDStream.foreachRDD(rdd => {

      // 获取 offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 业务逻辑处理
      rdd.foreachPartition(items => {
        items.foreach(line=>{
          println(line)
          kafkaProducer.value.send("ads_log2",line.value())
        })
      })

      // 等输出操作完成后提交offset
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    //启动
    ssc.start()
    ssc.awaitTermination()


  }

}
