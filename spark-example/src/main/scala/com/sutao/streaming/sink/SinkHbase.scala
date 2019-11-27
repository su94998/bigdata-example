package com.sutao.streaming.sink

import com.sutao.model.Student
import com.sutao.util.HbaseUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sutao on 2019/11/14.
  */
object SinkHbase {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SinkHbase")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineDstream = ssc.socketTextStream("hadoop151", 7777)

    val studentDstream=lineDstream.map(item=>{
      val arr=item.split(",")
      Student(arr(0).toLong,arr(1),Integer.valueOf(arr(2)))
    })

    studentDstream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreachPartition(items=>{
          val table = HbaseUtil.getHBaseTabel("student")
          val putList = new ArrayBuffer[Put]()
          items.foreach(item=>{
          val put = new Put(Bytes.toBytes(item.id))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(item.name))
          put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(item.age))
          putList += put
          })
          table.put(putList)
          table.close()
        })
      }
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
