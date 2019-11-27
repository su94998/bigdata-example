package com.sutao.streaming.app.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.sutao.model.AdsLog
import com.sutao.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

  //时间转换对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //redis保存用户点击次数的key
  val redisKey = "date-user-ads"

  //Redis中黑名单的key
  val blackList = "blackList"

  /**
    * 根据黑名单进行数据过滤
    *
    * @param adsLogDStream
    * @return
    */
  def filterDataByBlackList(adsLogDStream: DStream[AdsLog]): DStream[AdsLog] = {

    adsLogDStream.transform(rdd => {
      val filteredDataRDD: RDD[AdsLog] = rdd.mapPartitions(items => {
        // 每个分区获取黑名单进行过滤
        val jedis: Jedis = RedisUtil.getJedisClient
        val userids: util.Set[String] = jedis.smembers(blackList)
        jedis.close()
        items.filter(adsLog => {
          val userid: String = adsLog.userid
          !userids.contains(userid)
        })
      })
      filteredDataRDD
    })

  }

  /**
    * 检验数据，是否存在（每天每个用户对某个广告）点击数超过100的，则加入黑名单
    *
    * @param adsLogDStream 封装之后的数据流
    */
  def checkUserToBlackList(adsLogDStream: DStream[AdsLog]): Unit = {

    //1.转换维度
    val dateUserAdsOne: DStream[(String, Long)] = adsLogDStream.map(adsLog => {

      //获取时间并将时间转换为年月日
      val dateStr: String = sdf.format(new Date(adsLog.timestamp))

      //拼接Key
      val key = s"$dateStr:${adsLog.userid}:${adsLog.adid}"

      //返回
      (key, 1L)
    })

    //2.按照key聚合数据
    val dateUserAdsSum: DStream[(String, Long)] = dateUserAdsOne.reduceByKey(_ + _)

    //3.存入Redis
    dateUserAdsSum.foreachRDD(rdd => {

      //对每个分区进行处理
      rdd.foreachPartition(items => {

        //获取Jedis
        val jedis: Jedis = RedisUtil.getJedisClient

        //对每条数据进行处理
        items.foreach { case (key, count) =>

          // 写入Redis，每个批次key值的增量更新
          jedis.hincrBy(redisKey, key, count)

          //读出数据进行判断是否超过100
          if (jedis.hget(redisKey, key).toLong >= 100L) {

            //获取用户id
            val userId: String = key.split(":")(1)

            //若超过，则加入黑名单
            jedis.sadd(blackList, userId)
          }

        }

        //关闭jedis连接
        jedis.close()
      })

    })


  }

}
