package com.flink.hotItems.fromkafka

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
// 先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 定义一个窗口聚合结构样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItemsFromKafka {

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. read data
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")


    val dataStream= env.addSource(new FlinkKafkaConsumer[String]("test",new SimpleStringSchema(),properties))
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })// 文本数据已经按照时间排好序
   .assignAscendingTimestamps(_.timestamp * 1000L) // 转化为ms

    //3. transformation
    val processed = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5)) // 窗口大小为 1h，步长为 5min
      .aggregate(new CountAgg(),new WindowResult())   // 窗口聚合
      .keyBy(_.windowEnd)  // 按照窗口分组
      .process(new TopNHotItems(3))
    //4. sink
    processed.print()

    env.execute("hot items job")

  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{ // 第二个参数为累加器类型
  // 累加器初值
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，输出ItermViewCount
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit ={
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  // 定时器触发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个List Buffer中
    val allItems:ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }

    // 按照count大小排序，并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No.").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("========================================")

    // 控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}