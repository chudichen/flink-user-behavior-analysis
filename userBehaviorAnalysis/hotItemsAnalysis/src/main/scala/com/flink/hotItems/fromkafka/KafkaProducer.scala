package com.flink.hotItems.fromkafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


/**
 * Kafka生产者
 *
 * @author chudichen
 * @since 2020-05-08
 */
object KafkaProducer {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","localhost:9092")
    // 注意这里是生产者，使用序列化工具，而不是反序列化工具
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    val producer =new KafkaProducer[String,String](properties)

    // 从文件中读取数据并发送
    val bufferedSource = io.Source.fromFile("/home/chu/IdeaProjects/flink-UserBehaviorAnalysis-master/userBehaviorAnalysis/hotItemsAnalysis/src/main/resources/UserBehavior.csv")
    for(line<- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic, line)
      println("发送："+line)
//      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }
}
