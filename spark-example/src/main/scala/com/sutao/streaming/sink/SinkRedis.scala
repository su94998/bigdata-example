package com.sutao.streaming.sink

import com.sutao.util.{RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by sutao on 2019/11/14.
  */
object SinkRedis {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("word count sink redis").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("./ck_word_count")

    val lineDstream = ssc.socketTextStream("hadoop162", 7777)

    val wordAndCountStreams=lineDstream.flatMap(_.split(" ")).map((_, 1))

    val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    val stateDstream = wordAndCountStreams.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))

    stateDstream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreachPartition(items=>{
          // 获取连接
          val jedis: Jedis = RedisUtil.getJedisClient
          // 转换类型
          val keyCountMap: Map[String, String] = items.map { case (key, count) => (key, count.toString) }.toMap
          if(keyCountMap.size>0) {
            println(keyCountMap.size)
            // 导入隐式转换
            import scala.collection.JavaConversions._
            //执行批量插入
            jedis.hmset("word_count", keyCountMap)
            println("插入成功")
          }
          // 关闭连接
          jedis.close()
        })
      }
    })


    // 启动
    ssc.start()
    ssc.awaitTermination()



  }

}
