package com.sutao.flink.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * Created by sutao on 2019/11/4.
  */
object SinkKafka {

  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-sutao")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // 默认为latest从最新数据开始消费,earliest从头开始消费

    // Source
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("login", new SimpleStringSchema(), properties))

    // Transform
    val dataStream: DataStream[String] = kafkaStream.filter(_.contains("login"))

    // Sink
    val producerConfig = new Properties()
    producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    dataStream.addSink(new FlinkKafkaProducer011[String]( "sinkTest", new SimpleStringSchema(), producerConfig))
    dataStream.print()

    // 执行任务
    env.execute("kafka sink test")

  }

}
