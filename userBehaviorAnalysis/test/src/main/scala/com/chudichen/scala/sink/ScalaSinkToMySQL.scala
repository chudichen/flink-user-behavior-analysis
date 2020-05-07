package com.chudichen.scala.sink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author chudichen
 * @since 2020/5/6
 */
object ScalaSinkToMySQL {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("localhost", 7777)

    import org.apache.flink.api.scala._
    val studentStream = source.map(new MapFunction[String, Student] {
      override def map(value: String): Student = {
        val splits = value.split(",")
        Student(splits(0).toInt, splits(1), splits(2).toInt)
      }
    })

    studentStream.addSink(new SinkToMySQL())
    env.execute("ScalaSinkToMySQL")
  }
}
