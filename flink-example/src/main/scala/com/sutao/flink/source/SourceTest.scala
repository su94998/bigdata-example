package com.sutao.flink.source

import java.util.Properties

import com.sutao.flink.model.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Created by sutao on 2019/11/5.
  */
object SourceTest {

  def main(args: Array[String]): Unit = {

    // 1.从集合中获取数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1 = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
      ))


    // 2.从文件中获取数据
    val stream2 = env.readTextFile("E:\\toncent\\work\\bigdata-example\\flink-example\\src\\main\\resources\\sensor.txt")


    // 3.从kafka中获取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.162:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // 4.自定义source中获取数据
    val stream4 = env.addSource(new SensorSource())

   // stream1.print("stream1:").setParallelism(1)
   // stream2.print("stream2:").setParallelism(1)
   // stream3.print("stream3:").setParallelism(1)
    stream4.print("stream4:").setParallelism(1)

    env.execute()



  }

}

// 自定义source类
class SensorSource() extends SourceFunction[SensorReading]{

  // 定义一个flag：表示数据源是否还在正常运行
  var running: Boolean = true
  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 创建一个随机数发生器
    val rand = new Random()

    // 随机初始换生成10个传感器的温度数据，之后在它基础随机波动生成流数据
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    // 无限循环生成流数据，除非被cancel
    while(running){
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前的时间戳
      val curTime = System.currentTimeMillis()
      // 包装成SensorReading，输出
      curTemp.foreach(
        t => ctx.collect( SensorReading(t._1, curTime, t._2) )
      )
      // 间隔100ms
      Thread.sleep(100)
    }
  }

}
