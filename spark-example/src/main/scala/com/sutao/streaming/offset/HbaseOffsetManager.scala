package com.sutao.streaming.offset

import java.util.Properties

import com.sutao.util.{PropertiesUtil, ZkUtil}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sutao on 2019/10/16.
  *
  * 通过hbase管理kafka offset
  *
  * 只能保证数据不丢失，可能会重复处理
  *
  */
object HbaseOffsetManager {


  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeAPP").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkaParams: Map[String, Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.groupId"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest", //latest  earliest
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean) // 关闭自动提交offset
    )

    // 指定消费的主题
    val topic = "test1_log"

    // 获取offset
    val fromOffsets = getLastCommittedOffsets(topic, kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).toString)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, // 位置策略：该策略,会让Spark的Executor和Kafka的Broker均匀对应
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams, fromOffsets))

    kafkaDStream.foreachRDD(rdd => {

      // 获取rdd offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 业务逻辑处理
      rdd.foreachPartition(items => {
        items.foreach(println)
      })

      // 等输出操作完成后提交offset
      // 更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
      saveOffsets(topic, kafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG).toString, offsetRanges)

    })

    //启动
    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 获取最后一次提交的offset
    * @param topic
    * @param consumerGroupID
    * @return
    */
  def getLastCommittedOffsets(topic: String, consumerGroupID: String) = {

    // 最终封装返回的offset
    var fromOffsets: Map[TopicPartition, Long] = Map()

    // 获取kafka在zk中的分区数
    val zKNumberOfPartitionsForTopic = ZkUtil.getZkPartition(topic)

    // 获取hbase中保存的分区数和offset
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource("src/main/resources/hbase-site.xml")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf("stream_kafka_offsets"))
    val rowkey = topic + ":" + consumerGroupID
    val result: Result = table.get(new Get(rowkey.getBytes()))

    // hbase中保存的分区数
    var hbaseNumberOfPartitionsForTopic = 0

    // 如果hbase扫描程序的结果不为null，设置hbase的分区数
    if (!result.isEmpty) {
      hbaseNumberOfPartitionsForTopic = result.listCells().size()
      println("hbase中topic分区个数为："+hbaseNumberOfPartitionsForTopic)
    }

    // 如果hbase中分区不存在,则设置为zk分区最开始位置
    if (hbaseNumberOfPartitionsForTopic == 0) {
      for (partition <- 0 to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
      }
    } else if (zKNumberOfPartitionsForTopic > hbaseNumberOfPartitionsForTopic) {  // 如果zk分区数大于hbase保存的分区数，则给新增分区设置初始偏移量
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"), Bytes.toBytes(partition.toString)))
        fromOffsets += (new TopicPartition(topic, partition) -> fromOffset.toLong)
      }
      for (partition <- hbaseNumberOfPartitionsForTopic to zKNumberOfPartitionsForTopic - 1) {
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
      }
    } else { //获取最后一次提交的offset
      for (partition <- 0 to hbaseNumberOfPartitionsForTopic - 1) {
        val fromOffset = Bytes.toString(result.getValue(Bytes.toBytes("offsets"), Bytes.toBytes(partition.toString)))
        println("获取hbase中分区offset"+partition +"   "+fromOffset)
        fromOffsets += (new TopicPartition(topic, partition) -> fromOffset.toLong)
      }
    }

    conn.close()

    fromOffsets

  }


  /**
    * 保存offset
    *
    * @param topic
    * @param consumerGroupID
    * @param offsetRanges
    */
  def saveOffsets(topic: String, consumerGroupID: String, offsetRanges: Array[OffsetRange]) {

    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.addResource("src/main/resources/hbase-site.xml")

    val conn = ConnectionFactory.createConnection(hbaseConf)

    val table = conn.getTable(TableName.valueOf("stream_kafka_offsets"))

    val rowKey = topic + ":" + consumerGroupID

    val put = new Put(rowKey.getBytes)

    for (offset <- offsetRanges) {
      put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString), Bytes.toBytes(offset.untilOffset.toString))
    }

    table.put(put)
    conn.close()

  }

}
