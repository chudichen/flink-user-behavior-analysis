package com.chudichen.scala

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
 * @author chudichen
 * @since 2020/4/27
 */
object DataSetDemo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    crossFUnction(env)
  }

  def joinFunction(env: ExecutionEnvironment) = {
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "Tom"))
    info1.append((2, "Michael"))
    info1.append((3, "Scott"))
    info1.append((4, "Emma"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "Shanghai"))
    info2.append((2, "Beijing"))
    info2.append((3, "Chengdu"))
    info2.append((4, "Hangzhou"))

    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((left, right) => {
      (left._1, left._2, right._2)
    }).print()
  }

  def crossFUnction(env: ExecutionEnvironment) = {
    val info1 = List("曼联", "曼城")
    val info2 = List(3, 1, 0)

    import org.apache.flink.api.scala._
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()
  }
}
