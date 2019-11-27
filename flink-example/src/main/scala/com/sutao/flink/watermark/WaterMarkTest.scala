package com.sutao.flink.watermark

import java.util.Properties

import com.sutao.flink.model.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows

/**
  * Created by sutao on 2019/11/6.
  */
object WaterMarkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-sutao")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // 默认为latest从最新数据开始消费,earliest从头开始消费

    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream = kafkaStream
      .map(data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )
      val min=dataStream
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
        })
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //.timeWindow(Time.seconds(10),Time.seconds(5))
        .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.milliseconds(5000)))
      .reduce((result, data) => (data._1, result._2.min(data._2))) // 统计10秒内的最低温度值

    min.print("min data")
    dataStream.print("input data")

    env.execute()

  }

}
