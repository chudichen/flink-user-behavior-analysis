package com.flink.networkFlowAnalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
// 输入数据样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

// 窗口聚合结果样例类
case class UrlViewCount(url:String,windowEnd:Long,count:Long)


/**
  *  热门页面流量统计
  */
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("C:\\Users\\lenovo-aa\\Desktop\\flink\\flink-UserBehaviorAnalysis\\userBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data=>{
        val dataArray = data.split(" ")
        // 定义时间转换
        val simplmeDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simplmeDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime //ms
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60)) // 允许60s迟到数据
      .aggregate(new CountAgg(),new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()

    env.execute("network flow job")

  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator+1
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a+b
}

// 自定义窗口处理函数
class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

// 自定义排序输出处理函数
class TopNHotUrls(topSize:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  lazy val urlState:ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state",classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态中拿到数据
    val allUrlViews:ListBuffer[UrlViewCount] =new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()
    while(iter.hasNext){
      allUrlViews+=iter.next()
    }
    urlState.clear()

    val sortedUrlViews=allUrlViews.sortWith(_.count>_.count).take(topSize)

    // 格式化结果输出
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for(i<- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("No.").append(i+1).append(":")
        .append(" URL=").append(currentUrlView.url)
        .append(" 访问量=").append(currentUrlView.count).append("\n")
    }

    result.append("======================================================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}