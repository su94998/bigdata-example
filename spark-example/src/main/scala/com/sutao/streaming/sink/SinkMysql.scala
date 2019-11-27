package com.sutao.streaming.sink

import com.sutao.dao.MysqlManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sutao on 2019/11/14.
  */
object SinkMysql {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SinkMysql")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineDstream = ssc.socketTextStream("hadoop162", 7777)

    val wordAndCountStreams=lineDstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    wordAndCountStreams.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreachPartition(items=>{

          val paramsList = new ArrayBuffer[Array[Any]]()
          items.foreach(item=>{
            val params: Array[Any] = Array(
              item._1,item._2
            )
            paramsList+=params
          })

          // 批量新增
          MysqlManager.getMysqlManager.executeBatch("insert into word_count(word,count) values(?,?)",paramsList.toArray)

        })
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
