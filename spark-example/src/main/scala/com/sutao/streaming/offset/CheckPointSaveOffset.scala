package com.sutao.streaming.offset

import com.sutao.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sutao on 2019/10/16.
  * 通过checkpoint保存offset
  * 优点：简单易处理
  * 缺点：代码更改后则无法从检查点恢复
  */
object CheckPointSaveOffset {


  def main(args: Array[String]): Unit = {

    // 如果存在则通过检查点恢复状态数据和offset，不存在则创建一个StreamingContext
    val ssc = StreamingContext.getOrCreate("./ck11", functionToCreateContext _)
    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def functionToCreateContext(): StreamingContext = {

    // 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("CheckPointSaveOffset").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("./ck11")

    val topic = "test_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))

    kafkaDStream.foreachRDD(rdd=>{

      // 业务逻辑处理
      rdd.foreachPartition(items=>{
        items.foreach(println)
      })

    })

    ssc
  }

}
