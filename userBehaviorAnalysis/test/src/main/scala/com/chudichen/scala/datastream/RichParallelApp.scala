package com.chudichen.scala.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author chudichen
 * @since 2020/4/29
 */
object RichParallelApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.addSource(new CustomRichParallelSourceFunction)
    env.execute("RichParallelApp")
  }


}
