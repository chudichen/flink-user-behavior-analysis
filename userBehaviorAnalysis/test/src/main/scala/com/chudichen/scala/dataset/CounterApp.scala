package com.chudichen.scala.dataset

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @author chudichen
 * @since 2020/4/27
 */
object CounterApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    correctCounter(env)
  }

  def wrongCounter(env: ExecutionEnvironment) = {
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")
    data.map(new RichMapFunction[String, Long]() {
      var counter = 0L
      override def map(in: String) = {
        counter = counter + 1
        println("counter: " + counter)
        counter
      }
    }).setParallelism(4).print()
  }

  def correctCounter(env: ExecutionEnvironment) = {
    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")
    val info = data.map(new RichMapFunction[String, String]() {
      // step1:定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // step2:注册计数器
        getRuntimeContext.addAccumulator("ele.counts.scala", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })

    val filePath = "/home/chu/Desktop/sink-out"
    info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(3)
    val job = env.execute("counter")
    val accumulator  = job.getAccumulatorResult[Long]("ele.counts.scala")
    println(accumulator)
  }

}
