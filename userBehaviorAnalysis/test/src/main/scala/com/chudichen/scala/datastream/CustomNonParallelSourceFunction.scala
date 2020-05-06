package com.chudichen.scala.datastream

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author chudichen
 * @since 2020/4/29
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      sourceContext.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
