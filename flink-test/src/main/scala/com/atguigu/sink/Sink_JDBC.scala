package com.atguigu.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bean.WaterSensor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

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
    var conn: Connection = null
    var pstm: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456")
      pstm = conn.prepareStatement("insert into sensor_scala values(?,?,?)")
    }

    override def close(): Unit = {
      if (pstm != null)
        pstm.close()
      if (conn != null)
        conn.close()
    }

    override def invoke(value: WaterSensor, context: SinkFunction.Context): Unit = {
      pstm.setString(1, value.id)
      pstm.setLong(2, System.currentTimeMillis())
      pstm.setInt(3, value.vc)
      pstm.execute()
    }
  }

}
