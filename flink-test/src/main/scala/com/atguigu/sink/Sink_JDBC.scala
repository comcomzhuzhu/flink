package com.atguigu.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bean.WaterSensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName Sink_JDBC
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/13 10:46
  * @Version 1.0
  */
object Sink_JDBC {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textDS: DataStream[String] = env.readTextFile("flink-test/input/water.txt")

    val caseClassDS: DataStream[WaterSensor] = textDS.map((line: String) => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })

    caseClassDS.addSink(SinkMysql())

    env.execute()
  }

  case class SinkMysql() extends RichSinkFunction[WaterSensor] {
    val conn: Connection = null
    var pstm: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456")
      pstm = conn.prepareStatement("insert into sensor_temp values(?,?,?)")
    }

    override def close(): Unit = {
      pstm.close()
      conn.close()
    }

    override def invoke(value: WaterSensor, context: SinkFunction.Context): Unit = {
      pstm.setString(1, value.id)
      pstm.setLong(2, value.ts)
      pstm.setInt(3, value.vc)
      pstm.execute()
    }
  }

}
