package com.sutao.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by sutao on 2019/10/31.
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从调用时刻开始给env创建的每一个stream追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-sutao")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // 默认为latest从最新数据开始消费,earliest从头开始消费

    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("wc", new SimpleStringSchema(), properties))

    val dataStream: DataStream[(String, Int)] = kafkaStream
      .flatMap(_.split(" ")) //切分
      .filter(_.nonEmpty) // 过滤
      .map((_, 1)) // 转换元组
      .keyBy(0) // 分区
      .sum(1) // 聚合

    dataStream.print() // 打印

    // 执行任务
    env.execute("KafkaStreamWordCount")

  }


}
