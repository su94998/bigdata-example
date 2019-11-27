package com.sutao.flink.model

/**
  * Created by sutao on 2019/11/4.
  */
case class WordCount(word:String,count:Int)

// 传感器样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

// 支付信息样例类
case class PayInfo(userId: String, productNum:Long)
