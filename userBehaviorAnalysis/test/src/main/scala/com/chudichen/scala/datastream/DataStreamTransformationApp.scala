package com.chudichen.scala.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author chudichen
 * @since 2020/4/29
 */
object DataStreamTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    filterFunction(env)
    env.execute("DataStreamTransformationApp")
  }

  def filterFunction(env: StreamExecutionEnvironment) = {
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.filter(_ % 2 == 0).print().setParallelism(1)
  }

  def unionFunction()

}
