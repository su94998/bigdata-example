package com.sutao.flink.demand

import config.KafkaConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.{RandomUtils, TimeUtils}


object KafkaProducer {
  def main(args: Array[String]): Unit = {
    while(true){
      writeToKafka("test_a")
      Thread.sleep(10000)
    }
  }
  def writeToKafka(topic: String): Unit = {

    // kafka 配置
    val properties = KafkaConfig.properties()

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    // 控制发送条数
    val data = 200
    // 用户Id
    val userId = RandomUtils.randomList(data, data * 2)
    // 商品ID
    val goods = RandomUtils.randomList(data, data * 2)
    // 商品所属ID
    val goodsClass = RandomUtils.randomList(data, data * 2)
    // 动作
    val actions = RandomUtils.randomList(data, 12)

    val action = Array("pv", "buy", "cart", "fav", "pv", "pv", "pv", "pv", "pv", "pv", "pv", "pv")
    for (i <- 0 until data) {
      val record = new ProducerRecord[String, String](topic, "140100" + userId(i) + "," + "4402211" + goods(i) + "," + goodsClass(i) + "," + action(actions(i)) + "," + (System.currentTimeMillis() / 1000).toString)
//      println("140100" + userId(i) + "," + "4402211" + goods(i) + "," + goodsClass(i) + "," + action(actions(i)) + "," + (System.currentTimeMillis() / 1000).toString)
      producer.send(record)
    }
    producer.close()
    println(TimeUtils.tranTimeToString(System.currentTimeMillis()) + " 模拟发送了"+ data +"条数据")
  }

}
