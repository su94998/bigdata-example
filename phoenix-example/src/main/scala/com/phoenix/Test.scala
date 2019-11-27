package com.phoenix

import com.phoenix.util.PhoenixUtil

/**
  * Created by sutao on 2019/11/18.
  */
object Test {

  def main(args: Array[String]): Unit = {


    // 获取phoenix连接对象
    val connn = PhoenixUtil.getConnection();
    val state = connn.createStatement();
    val sql =
      s"""
         |select
         |"wqi"
         |from "national_water_quality_history_hour_data"
         |where "record_time" > '2019-10-01'
         |order by "record_time" desc limit 1
           """.stripMargin
    val rs = state.executeQuery(sql)
    var history_wqi:Double = 0
    while (rs != null && rs.next()) {
      history_wqi = rs.getString("wqi").toDouble
    }
    // 关闭连接
    PhoenixUtil.close(null, state, connn)


  }

}
