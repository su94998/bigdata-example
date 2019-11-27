package com.sutao.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
  * Created by sutao on 2019/11/14.
  *
  * 不带状态的WordCount统计，只会统计每条数据中的单词个数
  *
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    // 初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams = ssc.socketTextStream("hadoop162", 7777)

    // 将每一行数据做切分，形成一个个单词
    val wordStreams = lineStreams.flatMap(_.split(" "))

    // 将单词映射成元组（word,1）
    val wordAndOneStreams = wordStreams.map((_, 1))

    // 将相同的单词次数做统计
    val wordAndCountStreams = wordAndOneStreams.reduceByKey(_+_)

    //打印
    wordAndCountStreams.print()

    //启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
  }

}
