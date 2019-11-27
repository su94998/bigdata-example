package com.sutao.flink.demand

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop162:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource(new FlinkKafkaConsumer011[String]("test_a", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      // .timeWindow(Time.minutes(1), Time.seconds(10))
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合,统计窗口的商品浏览次数
      .keyBy(_.windowEnd) // 按照窗口分组
      .process(new TopNHotItems(3))

    // 4. sink：控制台输出
   // processedStream.print()

    // sink mysql
    processedStream.addSink(new MyJdbcSink())



    env.execute("hot items job")
  }
}

// 自定义预聚合函数
/**
  * UserBehavior输入参数类型
  * Long 累加器状态类型
  * Long 输出类型
  */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {

  // 每来一条数据累加器累加操作
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  // 初始化累加器的值
  override def createAccumulator(): (Long, Int) = (0L, 0)

  // 最终输出的结果
  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  // 累加器进行合并的操作
  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, ItemViewCount] {

  private var itemState: ListState[ItemViewCount] = _

  // 声明周期声明状态名称和类型
  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  // 每来一条数据处理方法
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, ItemViewCount]#Context, out: Collector[ItemViewCount]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器，延迟1毫秒触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, ItemViewCount]#OnTimerContext, out: Collector[ItemViewCount]): Unit = {
    // 将所有state中的数据取出，放到一个List Buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }

    // 按照count大小排序，并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    sortedItems.foreach(item=>{
      out.collect(item)
    })

    Thread.sleep(1000)

  }
}



// 自定义JdbcSink
class MyJdbcSink() extends RichSinkFunction[ItemViewCount]{

  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test", "root", "Root!!2019")
    insertStmt = conn.prepareStatement("INSERT INTO hotitems (item_id, count,record_time) VALUES (?,?,?)")
  }

  // 调用连接，执行sql
  override def invoke(data: ItemViewCount, context: SinkFunction.Context[_]): Unit = {
      insertStmt.setLong(1,data.itemId)
      insertStmt.setLong(2,data.count)
     // insertStmt.setString(3, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(data.windowEnd)))
    insertStmt.setTimestamp(3, new Timestamp(data.windowEnd))
      insertStmt.execute()
  }

  // 关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }

}

