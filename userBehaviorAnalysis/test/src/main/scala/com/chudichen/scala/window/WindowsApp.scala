package com.chudichen.scala.window

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

/**
 * @author chudichen
 * @since 2020/5/6
 */
object WindowsApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val text = env.socketTextStream("localhost", 9999)
    text.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))

    env.execute("WindowsApp")
  }
}
