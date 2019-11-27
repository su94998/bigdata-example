package com.sutao.streaming.state

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * Created by sutao on 2019/10/16.
  *
  * 有状态的WordCount统计，通过mapWithState实现，每批次将单词进行增量统计，性能高
  *
  * 经测试mapWithState无法从检查点恢复，错误提示如下：
  *
  * org.apache.spark.SparkException: This RDD lacks a SparkContext. It could happen in the following cases:
  * (1) RDD transformations and actions are NOT invoked by the driver, but inside of other transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because the values transformation and count action cannot be performed inside of the rdd1.map transformation. For more information, see SPARK-5063.
  * (2) When a Spark Streaming job recovers from checkpoint, this exception will be hit if a reference to an RDD not defined by the streaming job is used in DStream operations. For more information, See SPARK-13758.
  *
  *
  */
object MapWithState {

  def main(args: Array[String]): Unit = {

    // 如果存在则通过检查点恢复状态数据和offset，不存在则创建一个StreamingContext
    val ssc = StreamingContext.getOrCreate("./ck_01", functionToCreateContext _)
    //启动
    ssc.start()
    ssc.awaitTermination()

  }


  def functionToCreateContext(): StreamingContext = {

    // 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("StreamingWordCount1").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./ck_01")

    val lineStreams = ssc.socketTextStream("hadoop162", 7777)

    val wordDstream: DStream[(String, Int)] = lineStreams.flatMap(_.split(" ")).map((_, 1))


    val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          partition.foreach(println)
        })
      }
    })

    ssc

  }

}
