package com.sutao.streaming.app


import com.sutao.model.AdsLog
import com.sutao.streaming.app.handler.{BlackListHandler, DateAreaAdsTop3CountHandler, DateAreaCityAdsCountHandler}
import com.sutao.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by sutao on 2019/11/18.
  *
  * 基于用户广告点击行为实时分析
  *
  * 模拟数据发送kafka见com.sutao.streaming.app.mock.MockerRealTime
  *
  */
object RealTimeAPP {


  def main(args: Array[String]): Unit = {

    // 如果存在则通过检查点恢复状态数据和offset，不存在则创建一个StreamingContext
    val ssc = StreamingContext.getOrCreate("./ck-020", functionToCreateNewContext _)
    ssc.start()
    ssc.awaitTermination()

  }

  def functionToCreateNewContext(): StreamingContext = {

    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeAPP").setMaster("local[2]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("./ck-020")
    val topic = "ads_log"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, Array(topic))

    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map(record => {
      val splits: Array[String] = record.value().split(" ")
      AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    })

    // 1.黑名单过滤
    val filterdDStream: DStream[AdsLog] = BlackListHandler.filterDataByBlackList(adsLogDStream)

    // 用于后续多个需求计算的数据，缓存起来，提高性能
    filterdDStream.cache()

    // 2.统计每天每个省份每个城市每个广告的点击次数(使用有状态的算子更新)，存入Redis
    val dateAreaCityAdsToCount: DStream[(String, Long)] = DateAreaCityAdsCountHandler.getDateAreaCityAdsCount(filterdDStream, ssc)

    // 3.统计每天各省份 top3 热门广告，存入Redis
    DateAreaAdsTop3CountHandler.saveDateAreaAdsTop3CountToRedis(dateAreaCityAdsToCount,spark)

    // 校验数据是否需要加入黑名单，如果超过100次，则加入黑名单
    BlackListHandler.checkUserToBlackList(filterdDStream)

    ssc

  }


}
