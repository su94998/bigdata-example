package com.sutao.streaming.app

import java.text.SimpleDateFormat
import java.util.Date

import com.sutao.model.AdsLog
import com.sutao.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable.Map

/**
  * Created by sutao on 2019/11/20.
  *
  * 测试动态广播变量
  *
  * 无法从检查点恢复？
  *
  */
object Test {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  @volatile private var instance: Broadcast[Map[Int, String]] = null

  def main(args: Array[String]): Unit = {

    // 如果存在则通过检查点恢复状态数据和offset，不存在则创建一个StreamingContext
    val ssc = StreamingContext.getOrCreate("./ck-023", functionToCreateNewContext _)
    ssc.start()
    ssc.awaitTermination()

  }

  def functionToCreateNewContext(): StreamingContext = {

    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeAPP").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./ck-023")

    val topic = "ads_log"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))

    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map(record => {

      //取出其中的值
      val splits: Array[String] = record.value().split(" ")

      //封装为样例类对象
      AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })

    // 广播规则数据
    getInstance(ssc.sparkContext)


    val fileterxfgdg=adsLogDStream.transform(rdd=>{

      val current_time = sdf.format(new Date())
      val new_time = current_time.substring(14,16).toLong
      if(new_time % 2 == 0){
        update(rdd.sparkContext,true) //2分钟更新一次广播变量的内容
        println("[BroadcastWrapper] MonitorGame updated")
      }

      val gdgrg=rdd.map(item=>{
       if(instance.value.contains(1)){
         item
        }
        (false,"不包含1")
      })

      gdgrg
    })

    fileterxfgdg.foreachRDD(rdd=>{
      rdd.foreach(println)
    })


    ssc

  }

  /**
    * 更新instance;
    * @param sc
    * @param blocking
    */
  def update(sc: SparkContext, blocking: Boolean = false): Unit = {
    if (instance != null){
      instance.unpersist(blocking)
      instance = sc.broadcast(getMysqlData())
    }
  }

  /**
    * 初始化instance;
    * @param sc
    * @return
    */
  def getInstance(sc: SparkContext): Broadcast[Map[Int,String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(getMysqlData())
        }
      }
    }
    instance
  }

  def getMysqlData(): Map[Int, String] = {
    val games = Map[Int, String]()
    games += (1 -> "天龙八部")
    games
  }




}
