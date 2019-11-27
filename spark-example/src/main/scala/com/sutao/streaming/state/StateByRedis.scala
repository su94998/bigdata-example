package com.sutao.streaming.state

import com.sutao.util.RedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Created by sutao on 2019/11/26.
  *
  * 使用redis来维护中间状态
  *
  * 适用于key带过期删除的场景
  *
  */
object StateByRedis {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("StateByRedis").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineStreams = ssc.socketTextStream("hadoop162", 7777)

    val wordCountDstream= lineStreams.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    wordCountDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        if(partition.nonEmpty) {
          val jedis: Jedis = RedisUtil.getJedisClient
          partition.foreach{case (key:String,value:Int) =>
            val lastVal = jedis.get(key)
            if(lastVal!=null){
              val res = lastVal.toInt + value
              jedis.set(key,String.valueOf(res))
              jedis.expire(key,20)
            }else{
              jedis.set(key,String.valueOf(value))
              jedis.expire(key,20)
            }
          }
          jedis.close()
        }
      })
    })


    ssc.start()
    ssc.awaitTermination()



  }

}
