package com.sutao.flink.windown

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
  * Created by sutao on 2019/11/12.
  */
object Test {

  def main(args: Array[String]): Unit = {
    // 环境
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dstream: DataStream[String] = env.socketTextStream("localhost",7777)
    val textWithTsDstream: DataStream[(String, Long, Int)] = dstream.map
    { text =>
      val arr: Array[String] = text.split(" ")
      (arr(0), arr(1).toLong, 1)
    }
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] =
      textWithTsDstream.assignTimestampsAndWatermarks(new
          BoundedOutOfOrdernessTimestampExtractor[(String, Long,
            Int)](Time.milliseconds(1000)) {
        override def extractTimestamp(element: (String, Long, Int)): Long = {
          return element._2
        }
      })
    val textKeyStream: KeyedStream[(String, Long, Int), Tuple] =
      textWithEventTimeDstream.keyBy(0)

    textKeyStream.print("textkey:")

    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] =
      textKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))



    env.execute()
  }
}




