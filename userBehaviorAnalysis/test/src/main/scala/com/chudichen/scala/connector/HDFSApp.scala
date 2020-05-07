package com.chudichen.scala.connector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
 * @author chudichen
 * @since 2020/5/6
 */
object HDFSApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val filePath = "file:///home/chu/Desktop"
    val sink = new BucketingSink[String](filePath)

    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setBatchRolloverInterval(2000)
    data.addSink(sink)
    env.execute("HDFSApp")
  }

}
