package com.sutao.util

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}


class KafkaSink[K,V](createproducer:() => KafkaProducer[K,V]) extends Serializable{

  lazy val producer = createproducer()

  def send(topic:String,key:K,value:V):Future[RecordMetadata] = producer.send(new ProducerRecord[K,V](topic,key,value))

  def send(topic: String, value: V): Future[RecordMetadata] = producer.send(new ProducerRecord[K, V](topic, value))
}


/**
  * Created by sutao on 2019/10/16.
  */

object KafkaSink {

  import scala.collection.JavaConversions._
  def apply[K,V](config:Map[String,Object]):KafkaSink[K,V] = {
    val createProducerFunc  = () => {
      val producer = new KafkaProducer[K,V](config)
      sys.addShutdownHook({
        producer.close()
      })
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)

}
