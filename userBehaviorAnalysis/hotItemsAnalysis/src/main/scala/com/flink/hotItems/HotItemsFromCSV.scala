package com.flink.hotItems

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 * 从CSV中统计热门物品
 *
 * @author chudichen
 * @since 2020-05-08
 */
object HotItemsFromCSV{

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. read data
    val filePath = "/home/chu/IdeaProjects/flink-UserBehaviorAnalysis-master/userBehaviorAnalysis/hotItemsAnalysis/src/main/resources/UserBehavior.csv"

    import org.apache.flink.api.scala._
    val dataStream: DataStream[UserBehavior] = env.readTextFile(filePath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong,
          dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      // 由于我们的数据源的数据已经经过整理，没有乱序，即事件的时间是单调递增的，所以可以将每天数据的业务时间当作Watermark
      // 真是业务场景一般都是乱序的，所以一般不用assignAscendingTimestamps，而是使用 BoundedOutOfOrdernessTimestampExtractor。
      // 转为ms
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transformation
    val processed = dataStream
      // 过滤出点击事件，院士数据中存在点击、购买、收藏、喜欢等行为
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      // 窗口大小为1H，步长为5min，分别要统计[09:00, 10:00]，[09:05, 10:05]等窗口的商品点击量
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 提前窗口聚合，提前
      .aggregate(new CountAgg(),new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    // 4. sink
    processed.print()
    env.execute("HotItemsFromCSV")
  }
}

/**
 * 用户行为输入
 *
 * @param userId 用户Id
 * @param itemId 物品Id
 * @param categoryId 分类Id
 * @param behavior 行为
 * @param timestamp 时间戳
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
 * 窗口聚合结构样例类
 *
 * @param itemId 物品Id
 * @param windowEnd 窗口
 * @param count 数量
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

/**
 * 自定义TopN处理函数
 *
 * @param topItemSize 取前几个
 */
class TopNHotItems(topItemSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }


  // 在processElement方法中，每当受到一条数据ItemViewCount，就注册一个windowEnd + 1的定时器（Flink会自动忽略同一时间的重复注册）
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器出发时，对所有数据排序，并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    // 按照count大小排序，并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topItemSize)
    // 清空状态
    itemState.clear()

    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
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