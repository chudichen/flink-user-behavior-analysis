package com.chudichen.scala.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author chudichen
 * @since 2020/4/29
 */
object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    nonParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }
}
