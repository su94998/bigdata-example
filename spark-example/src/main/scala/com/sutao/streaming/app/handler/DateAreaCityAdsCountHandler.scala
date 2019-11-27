package com.sutao.streaming.app.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.sutao.model.AdsLog
import com.sutao.util.RedisUtil
import org.apache.spark.streaming.{State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DateAreaCityAdsCountHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //定义RedisKey
  val redisKey = "day-area-city-ads"

  /**
    * 将统计完的结果保存至Redis
    *
    * @param dateAreaCityAdsToCount 统计完的结果
    */
  def saveDataToRedis(dateAreaCityAdsToCount: DStream[(String, Long)]): Unit = {

    dateAreaCityAdsToCount.foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        if (items.nonEmpty) {
          val jedis: Jedis = RedisUtil.getJedisClient
          // 转换类型
          val keyCountMap: Map[String, String] = items.map { case (key, count) => (key, count.toString) }.toMap
          // 导入隐式转换
          import scala.collection.JavaConversions._
          // 执行批量插入
          jedis.hmset(redisKey, keyCountMap)
          jedis.close()
        }

      })
    })

  }

  /**
    * 对于过滤后的数据进行按时间地区城市广告计数
    *
    * @param filterdDStream 过滤后的数据
    * @return
    */
  def getDateAreaCityAdsCount(filterdDStream: DStream[AdsLog],ssc:StreamingContext): DStream[(String, Long)] = {

    //1.维度转换
    val dateAreaCityAdsToOne: DStream[(String, Long)] = filterdDStream.map(adsLog => {

      //获取时间
      val dateStr: String = sdf.format(new Date(adsLog.timestamp))

      //拼接key
      val key = s"$dateStr:${adsLog.area}:${adsLog.city}:${adsLog.adid}"

      //返回
      (key, 1L)
    })


/*
    val initAreaCityAdsCountRDD = ssc.sparkContext.parallelize(List[(String, Long)]())

    val mappingFunc = (key: String, one: Option[Long], state: State[Long]) => {
      val sum = one.getOrElse(0L) + state.getOption.getOrElse(0L)
      val output = (key, sum)
      state.update(sum)
      output
    }

    // mapWithState 增量统计
    val dateAreaCityAdsToCount = dateAreaCityAdsToOne.mapWithState(StateSpec.function(mappingFunc).initialState(initAreaCityAdsCountRDD))
*/



    // updateStateByKey 全量统计
    val dateAreaCityAdsToCount: DStream[(String, Long)] = dateAreaCityAdsToOne.updateStateByKey((seq: Seq[Long], state: Option[Long]) => {

      //当前批次求和
      val sum: Long = seq.sum

      //加入之前保存的状态
      val newState: Long = state.getOrElse(0L) + sum

      Some(newState)
    })

    // 将每天每个地区每个城市每个广告的点击次数写入Redis
    DateAreaCityAdsCountHandler.saveDataToRedis(dateAreaCityAdsToCount)


    dateAreaCityAdsToCount

  }

}
