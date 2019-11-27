package com.sutao.flink.windown

import java.util.Properties

import com.sutao.flink.model.PayInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._

/**
  * Created by sutao on 2019/11/5.
  *
  * CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果
  *
  * 注意：CountWindow的window_size指的是相同Key的元素的个数，不是输入的所有元素的总数
  */
object CountWindow {

  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-sutao")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // 默认为latest从最新数据开始消费,earliest从头开始消费

    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("pay", new SimpleStringSchema(), properties))

    // 统计每10个用户购买行为中的购买总数
    val counts = kafkaStream
      .map(line => {
        val dataArray = line.split(",")
        PayInfo(dataArray(0),dataArray(1).toLong)
      })
      .keyBy(0) // userId分组
      .countWindow(10) // 每当key达到10次时就计算元素内容
      .sum(1) // 计算购买数量


    // 统计用户每5次购买行为的最近十次购买总数
    val counts1 = kafkaStream
      .map(line => {
        val dataArray = line.split(",")
        PayInfo(dataArray(0),dataArray(1).toLong)
      })
      .keyBy(0) // userId分组
      .countWindow(10,5) // 每当key达到5次的时候就计算最近10次的元素内容
      .sum(1) // 计算购买数量


   // counts.print()
    counts1.print()

    env.execute()


  }

}
