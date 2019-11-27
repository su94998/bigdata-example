package com.sutao.flink.windown

import java.util.Properties

import com.sutao.flink.model.PayInfo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows

/**
  * Created by sutao on 2019/11/5.
  *
  * TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算
  *
  * Flink默认的时间窗口根据Processing Time 进行窗口的划分，将Flink获取到的数据根据进入Flink的时间划分到不同的窗口中
  */
object TimeWindow {

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

    // 统计每一分钟中用户购买的商品的总数
    val counts = kafkaStream
      .map(line => {
        val dataArray = line.split(",")
        PayInfo(dataArray(0),dataArray(1).toLong)
      })
      .keyBy(0) // userId分组
      .timeWindow(Time.minutes(1)) // 1分钟的滚动窗口宽度
      .sum(1) // 计算购买数量


    // 每30秒计算一次最近一分钟用户购买的商品总数
    val counts1 = kafkaStream
      .map(line => {
        val dataArray = line.split(",")
        PayInfo(dataArray(0),dataArray(1).toLong)
      })
      .keyBy(0) // userId分组
      .timeWindow(Time.minutes(1),Time.seconds(30)) // 1分钟的窗口,30秒的滑动
      .sum(1) // 计算购买数量


    // 每个用户在活跃期间总共购买的商品数量,如果用户30秒内没有活动则视为会话断开
    val counts2 = kafkaStream
      .map(line => {
        val dataArray = line.split(",")
        PayInfo(dataArray(0),dataArray(1).toLong)
      })
      .keyBy(0) // userId分组
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
      .sum(1) // 计算购买数量



    //counts.print()
    //counts1.print()
    counts2.print()

    env.execute()


  }

}
