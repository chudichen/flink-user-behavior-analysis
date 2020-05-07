package com.chudichen.scala.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * Kafka生产者
 *
 * @author chudichen
 * @since 2020/5/6
 */
object FlinkKafkaProducerApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val topic = "test"

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    import org.apache.flink.api.scala._
    val kafkaSink = new FlinkKafkaProducer[String](
      topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      properties)

    data.addSink(kafkaSink)
    env.execute("FlinkKafkaProducerApp")
  }
}
