package com.sutao.flink.process


import com.sutao.flink.model.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Created by sutao on 2019/11/6.
  *
  * 状态编程
  *
  * 同一个传感器一段时间内温度连续上升则输出报警信息
  *
  */
object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("106.53.66.91", 6679)

    val dataStream = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val processedStream = dataStream.keyBy(_.id)
      .process( new TempIncreAlert() )

    dataStream.print()
    processedStream.print("温度连续上升")

    env.execute()


  }

}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]) )
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("currentTimer", classOf[Long]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update( value.temperature )

    val curTimerTs = currentTimer.value()


    if( value.temperature < preTemp || preTemp == 0.0 ){
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer( curTimerTs )
      currentTimer.clear()
    } else if ( value.temperature > preTemp && curTimerTs == 0 ){
      // 温度上升且没有设过定时器，则注册定时器，5秒之后输出报警信息
      val timerTs = ctx.timerService().currentProcessingTime() + 5000L
      ctx.timerService().registerProcessingTimeTimer( timerTs )
      currentTimer.update( timerTs )
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 输出报警信息
    out.collect( "传感器 id 为: " + ctx.getCurrentKey + "的温度值已经连续上升了。")
    currentTimer.clear()
  }
}
