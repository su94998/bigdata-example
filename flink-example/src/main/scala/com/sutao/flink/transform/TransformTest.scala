package com.sutao.flink.transform

import com.sutao.flink.model.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * Created by sutao on 2019/11/5.
  */
object TransformTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读入数据
    val inputStream = env.readTextFile("E:\\toncent\\work\\bigdata-example\\flink-example\\src\\main\\resources\\sensor.txt")

    // Transform操作
    val dataStream = inputStream
      .map(
        data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        }
      )

    // 1. 聚合操作
    val stream1 = dataStream
      .keyBy("id")
      //.sum("temperature")
      //.reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
      .reduce((x, y) => SensorReading(x.id, x.timestamp, y.temperature + x.temperature))

    // 2. 分流，根据温度是否大于30度划分
    val splitStream = dataStream
      .split( sensorData => {
        if( sensorData.temperature > 30 ) Seq("high") else Seq("low")
      } )

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合并两条流
    val warningStream = highTempStream.map( sensorData => (sensorData.id, sensorData.temperature) )
    val connectedStreams = warningStream.connect(lowTempStream)

    val coMapStream = connectedStreams.map(
      warningData => ( warningData._1, warningData._2, "high temperature warning" ),
      lowData => ( lowData.id, "healthy" )
    )

    val unionStream = highTempStream.union(lowTempStream)

    // 过滤
    dataStream.filter(_.id.startsWith("sensor_1")).print()

    // 输出数据
    stream1.print()
    //    dataStream.print()
    //    highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")
    //    unionStream.print("union")

    env.execute("transform test job")

  }

}
