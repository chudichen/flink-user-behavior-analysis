package com.chudichen.scala.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @author chudichen
 * @since 2020/5/6
 */
class SinkToMySQL extends RichSinkFunction[Student] {

  var connection: Connection = null
  var pstmt: PreparedStatement = null

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    println("Invoke MySQL sink")
    pstmt.setInt(1, value.id)
    pstmt.setString(2, value.name)
    pstmt.setInt(3, value.age)
    pstmt.executeUpdate()
  }

  def getConnection(): Connection = {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url = "jdbc:mysql://localhost:3306/study_demo"
      DriverManager.getConnection(url, "root", "root")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection()
    val sql = "insert into student(id, name, age) values (?,?,?)"
    pstmt = connection.prepareStatement(sql)
    println("Open")
  }

  override def close(): Unit = {
    super.close()

    if (pstmt != null) {
      pstmt.close()
    }

    if (connection != null) {
      connection.close()
    }
  }
}
