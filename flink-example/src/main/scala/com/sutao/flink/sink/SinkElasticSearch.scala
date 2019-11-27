package com.sutao.flink.sink

import java.util
import java.util.Properties

import com.sutao.flink.model.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.elasticsearch.client.Requests

/**
  * Created by sutao on 2019/11/4.
  */
object SinkElasticSearch {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 指定时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 每隔5000 ms进行启动一个检查点
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop162:9092,hadoop163:9092,hadoop164:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-sutao")
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // 默认为latest从最新数据开始消费,earliest从头开始消费

    // Source
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // transform
    // kafka日志格式 sensor_1,1547718199,35.80018327300259
    val dataStream = kafkaStream
      .map(
        line => {
          val dataArray = line.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
        }
      )

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.1.162", 9200))

    // 创建一个esSink 的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          println("saving data: " + element)
          // 包装成一个Map或者JsonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", element.id)
          json.put("temperature", element.temperature.toString)
          json.put("ts", element.timestamp.toString)

          // 创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          // 利用index发送请求，写入数据
          indexer.add(indexRequest)
          println("data saved.")
        }
      }
    )

    /*     必须设置flush参数     */
    // 刷新前缓冲的最大动作量
    esSinkBuilder.setBulkFlushMaxActions(1)
    // 刷新前缓冲区的最大数据大小（以MB为单位）
    esSinkBuilder.setBulkFlushMaxSizeMb(500)
    // 缓冲操作的数量或大小如何都要刷新的时间间隔
    esSinkBuilder.setBulkFlushInterval(5000)

    // sink
    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")
  }

}
