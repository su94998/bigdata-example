package com.sutao.util

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * Created by sutao on 2019/10/12.
  */
object MyKafkaUtil {

  private val properties: Properties = PropertiesUtil.load("config.properties")


  private val kafkaParams:Map[String, Object] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.broker.list"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> properties.getProperty("kafka.groupId"),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",//latest  earliest
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG->(false: java.lang.Boolean)
  )

  def getKafkaStream(ssc: StreamingContext, topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {

    val DStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )

    DStream
  }



}
