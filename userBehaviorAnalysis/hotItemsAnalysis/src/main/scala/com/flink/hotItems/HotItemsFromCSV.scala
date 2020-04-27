package com.flink.hotItems


import java.sql.Timestamp

import com.flink.hotItems.fromkafka.{CountAgg, TopNHotItems, WindowResult}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 定义一个窗口聚合结构样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)


/**
  * 需求：每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品。
  * 步骤：
  *       1.抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
  *       2.过滤出点击行为数据
  *       3.按一小时的窗口大小，每 5 分钟统计一次，做滑动窗口聚合（ Sliding Window）
  *       4.输出每个窗口中点击量前 N 名的商品
  *
  *
  */
object HotItems {

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. read data
    val dataStream: DataStream[UserBehavior] = env.readTextFile("C:\\Users\\lenovo-aa\\Desktop\\flink\\flink-UserBehaviorAnalysis\\userBehaviorAnalysis\\hotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      // 由于我们的数据源的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做 Watermark
      // 真实业务场景一般都是乱序的，所以一般不用assignAscendingTimestamps，而是使用 BoundedOutOfOrdernessTimestampExtractor。
      .assignAscendingTimestamps(_.timestamp * 1000L) // 转化为ms

    //3. transformation
    val processed = dataStream
      .filter(_.behavior == "pv") // 过滤出点击事件，原始数据中存在点击、购买、收藏、喜欢各种行为的数据
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5)) // 窗口大小为 1h，步长为 5min，分别要统计[09:00,  10:00),  [09:05,  10:05),  [09:10,10:10)…等窗口的商品点击量。
      .aggregate(new CountAgg(),new WindowResult())   // 提前窗口聚合，提前聚合掉数据，减少state的存储压力，较之.apply(WindowFunction wf)会将窗口中的数据都存储下来，所以高效地多
      .keyBy(_.windowEnd)  // 按照窗口（即窗口关闭时刻）分组
      .process(new TopNHotItems(3))
    //4. sink
    processed.print()

    env.execute("hot items job")

  }
}

// 自定义预聚合函数，每出现一条记录就+1
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

  // 在 processElement方法中，每当收到一条数据ItemViewCount，就注册一个 windowEnd+1 的定时器（ Flink 框架会自动忽略同一时间的重复注册）
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