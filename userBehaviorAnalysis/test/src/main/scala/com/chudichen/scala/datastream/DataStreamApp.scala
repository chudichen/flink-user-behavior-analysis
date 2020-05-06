package com.chudichen.scala.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 数据流
 *
 * @author chudichen
 * @since 2020/4/29
 */
object DataStreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    socketFunction(env)
    env.execute("DataStreamApp")
  }

  def socketFunction(env: StreamExecutionEnvironment) = {
    val data = env.socketTextStream("localhost", 9999)
    data.print().setParallelism(1)
  }

}
