package com.sutao.streaming.app.handler

import com.sutao.model.{DateAreaCityAdsToCount, DateAreaTOP3}
import com.sutao.util.RedisUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object DateAreaAdsTop3CountHandler {

  /**
    * 统计每天每个地区点击量在前3的广告并存入Redis
    *
    * @param dateAreaCityAdsToCount
    * @return
    */
  def saveDateAreaAdsTop3CountToRedis(dateAreaCityAdsToCount: DStream[(String, Long)], spark: SparkSession) = {

    val areaDateTOP3DStream = dateAreaCityAdsToCount.transform(rdd => {

      // 将rdd转换ds，使用spark sql来进行计算
      val dateAreaCityAdsToCountRDD = rdd.map { case (keyStr, count) =>
        val keySplited = keyStr.split(":")
        DateAreaCityAdsToCount(keySplited(0), keySplited(1), keySplited(2), keySplited(3), count)
      }

      import spark.implicits._
      dateAreaCityAdsToCountRDD.toDS().createOrReplaceTempView("tmp_date_area_city_ads_count")


      // 开窗函数统计top3
      val areaDateTOP3RDD = spark.sql(
        s"""
           |select
           |*
           |from(
           |select
           |dt,
           |area,
           |adid,
           |click_count,
           |ROW_NUMBER() OVER(PARTITION BY dt,area ORDER BY click_count desc) rank
           |from(
           |select
           |dt,
           |area,
           |adid,
           |sum(click_count) click_count
           |from
           |tmp_date_area_city_ads_count
           |group by dt,area,adid
           |)
           |)
           |where rank<=3
         """.stripMargin).as[DateAreaTOP3].rdd

      areaDateTOP3RDD

    })

    /**
      * DateAreaTOP3(2019-11-22,华北,4,19,1)
      * DateAreaTOP3(2019-11-22,华北,5,19,2)
      * DateAreaTOP3(2019-11-22,华北,6,19,3)
      *
      * DateAreaTOP3(2019-11-22,华东,1,19,1)
      * DateAreaTOP3(2019-11-22,华东,2,13,2)
      * DateAreaTOP3(2019-11-22,华东,5,10,3)
      *
      * DateAreaTOP3(2019-11-22,华南,4,18,1)
      * DateAreaTOP3(2019-11-22,华南,6,17,2)
      * DateAreaTOP3(2019-11-22,华南,1,13,3)
      */


  }

}
