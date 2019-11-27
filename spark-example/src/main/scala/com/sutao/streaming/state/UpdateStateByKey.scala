package com.sutao.streaming.state

import com.sutao.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by sutao on 2019/10/16.
  *
  * 有状态的WordCount统计，通过updateStateByKey实现，每批次将单词进行全量统计，性能较低
  *
  * 大数据量的时候不适合，尤其是key维度比较高，value状态有比较大的时候
  *
  * redis适合要维护key超时删除的机制的时候使用，alluxio适合超大吞吐量
  *
  */
object UpdateStateByKey {

  def main(args: Array[String]): Unit = {

    // 如果存在则通过检查点恢复状态数据和offset，不存在则创建一个StreamingContext
    val ssc = StreamingContext.getOrCreate("./ck1008611", functionToCreateContext _)
    //启动
    ssc.start()
    ssc.awaitTermination()

  }

  def functionToCreateContext(): StreamingContext = {

    // 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("./ck1008611")

    val topic = "test1_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))

    val lineDstream: DStream[String] = kafkaDStream.map(_.value())

    val wordDstream: DStream[(String, Int)] = lineDstream.flatMap(_.split(" ")).map((_,1))

    val stateDstream=wordDstream.updateStateByKey((currValues:Seq[Int],preValue:Option[Int])=>{
      val sum = currValues.sum
      Some(sum + preValue.getOrElse(0))
    }).checkpoint(Seconds(20)) //设置checkpoint频率，5-10倍的滑动窗口

    stateDstream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreachPartition(partition=>{
          partition.foreach(println)
        })
      }
    })

    ssc
  }

}
