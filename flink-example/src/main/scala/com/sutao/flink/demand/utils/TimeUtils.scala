package com.sutao.flink.demand.utils

import java.text.SimpleDateFormat
import java.util.Date


class TimeUtils{

}

// 时间处理工具类
object TimeUtils {

  // 字符串戳转时间
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime
    tim
  }

  // 时间戳转字符串
  def tranTimeToString(tm:Long) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = fm.format(new Date(tm))
    time
  }


}
