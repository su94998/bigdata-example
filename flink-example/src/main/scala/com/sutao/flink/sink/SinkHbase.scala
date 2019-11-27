package com.sutao.flink.sink

import java.util.Properties

import com.sutao.flink.model.WordCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by sutao on 2019/11/4.
  */
object SinkHbase {

  def main(args: Array[String]): Unit = {

    // 创建流处理环境
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

    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("wc", new SimpleStringSchema(), properties))

    val dataStream= kafkaStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .map(item=>{
        WordCount(item._1,item._2)
      })

    // sink
    dataStream.addSink(new MyHbaseSink())

    // 执行任务
    env.execute("hbase sink test")

  }


}


class MyHbaseSink() extends RichSinkFunction[WordCount]{

  var table: Table = _

  // 初始化，获取连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 创建HBase配置
    val config = HBaseConfiguration.create
    // 设置HBase参数
    config.set("hbase.zookeeper.property.clientPort","2181")
    config.set("hbase.zookeeper.quorum","hadoop162,hadoop163,hadoop164")
    // 创建HBase连接
    val connection = ConnectionFactory.createConnection(config)
    // 获取HBaseTable
    table = connection.getTable(TableName.valueOf("test"))
  }

  // 调用连接，执行操作
  override def invoke(data: WordCount, context: SinkFunction.Context[_]): Unit = {
    val put = new Put(Bytes.toBytes(data.word));
    put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("count"), Bytes.toBytes(data.count));
    table.put(put)
  }

  override def close(): Unit = {
    table.close
  }

}
