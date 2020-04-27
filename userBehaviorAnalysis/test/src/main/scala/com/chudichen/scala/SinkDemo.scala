package com.chudichen.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
 * @author chudichen
 * @since 2020/4/27
 */
object SinkDemo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data = 1 to 10
    val text = env.fromCollection(data)
    val filePath = "/home/chu/Desktop/sink-out"
    text.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE)
    env.execute("Sink")
  }
}
