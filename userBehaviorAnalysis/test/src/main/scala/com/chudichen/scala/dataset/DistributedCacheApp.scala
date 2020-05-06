package com.chudichen.scala.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * 分布式缓存
 *
 * @author chudichen
 * @since 2020/4/29
 */
object DistributedCacheApp {

  val cacheFile = "cache-file"

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val filePath = "/home/chu/Desktop/people.csv"
    // 注册一个本地文件
    env.registerCachedFile(filePath, cacheFile)

    import org.apache.flink.api.scala._
    val data = env.fromElements("hadoop", "spark", "flink", "storm")

    data.map(new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile(cacheFile)
        val lines = FileUtils.readLines(file)

        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()
  }
}
