package com.sutao.flink.demand.config

import java.util.Properties

// kafka 配置信息
class KafkaConfig {

}

object KafkaConfig{

  // 测试环境配置
  def properties() :Properties ={
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop162:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties
  }
}
