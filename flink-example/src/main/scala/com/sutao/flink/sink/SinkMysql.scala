package com.sutao.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.sutao.flink.model.WordCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by sutao on 2019/11/4.
  */
object SinkMysql {

  def main(args: Array[String]): Unit = {

    // 创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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
    dataStream.addSink(new MyJdbcSink())

    // 执行任务
    env.execute("mysql sink test")

  }


}


// 自定义JdbcSink
class MyJdbcSink() extends RichSinkFunction[WordCount]{

  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test", "root", "Root!!2019")
    insertStmt = conn.prepareStatement("INSERT INTO word_count (word, count) VALUES (?,?)")
    updateStmt = conn.prepareStatement("UPDATE word_count SET count = ? WHERE word = ?")
  }

  // 调用连接，执行sql
  override def invoke(data: WordCount, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setInt(1,data.count)
    updateStmt.setString(2,data.word)
    updateStmt.execute()
    // 如果update没有查到数据，那么执行插入语句
    if(updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1,data.word)
      insertStmt.setInt(2,data.count)
      insertStmt.execute()
    }
  }

  // 关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}


