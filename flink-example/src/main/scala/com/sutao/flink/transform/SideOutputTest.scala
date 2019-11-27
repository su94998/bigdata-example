package com.sutao.flink.transform

import com.sutao.flink.model.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Created by sutao on 2019/11/7.
  *
  * 侧输出流，用于将一条流数据拆分为多条输出，通过标签获取相应的流做处理
  */
object SideOutputTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop162", 7777)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
      } )

    val processedStream = dataStream
      .process( new FreezingAlert() )


    //    dataStream.print("input data")
    processedStream.print("processed data")
    processedStream.getSideOutput( new OutputTag[String]("freezing alert") ).print("alert data")


    env.execute("side output test")
  }

}



class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 冰点报警，如果小于32F，输出报警信息到侧输出流
    if( value.temperature < 32.0 ){
      ctx.output( new OutputTag[String]( "freezing alert" ), "freezing alert for " + value.id )
    }
    // 所有数据直接常规输出到主流
    out.collect( value )
  }

}
