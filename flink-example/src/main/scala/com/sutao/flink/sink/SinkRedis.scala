package com.sutao.flink.sink

import java.util.Properties

import com.sutao.flink.model.WordCount
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * Created by sutao on 2019/11/4.
  */
object SinkRedis {

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

    val dataStream = kafkaStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .map(item => {
        WordCount(item._1, item._2)
      })

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop162")
      .setPort(6379)
      .build()

    val sink = new RedisSink[WordCount](conf, new MyRedisMapper)

    // sink
    dataStream.addSink(sink)

    // 执行任务
    env.execute("redis sink test")

  }

}



class MyRedisMapper() extends RedisMapper[WordCount]{

  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  // 定义保存到redis的value
  override def getValueFromData(data: WordCount): String =data.count.toString

  // 定义保存到redis的key
  override def getKeyFromData(data: WordCount): String = data.word

}
