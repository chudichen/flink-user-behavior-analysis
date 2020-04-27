package com.flink.pageView

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  *  统计整个网站数据中页面浏览量或点击量
  *
  */
// 先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)


object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 升序数据，直接分配时间戳
      .filter(_.behavior=="pv")  // 只统计pv操作
      .map(data=>("pv",1))
//      .keyBy(_._1)  // 哑key
//      .timeWindow(Time.hours(1))
//      .sum(1)
        .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))  // 先用哑key可以
        .sum(1)



    dataStream.print("pv count")

    env.execute("page view job")




  }
}
