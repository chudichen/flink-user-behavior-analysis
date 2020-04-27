package com.flink.UniqueVisistor

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
// 定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 定义输出数据样例类
case class UvCount(windowEnd:Long,uvCount:Long)

/**
  *  Uv 简单统计
  *
  */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 升序数据，直接分配时间戳
      .filter(_.behavior=="pv")  // 只统计pv操作
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())
    dataStream.print()
    env.execute("unique visitor job")
  }
}

class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var idSet = Set[Long]()
    for(userBehavior <- input){
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}
