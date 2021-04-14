package com.atguigu.day5work

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
  * @ObjectName Work1
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/14 8:39
  * @Version 1.0
  */
object Work1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val pro = new Properties()
    pro.put("bootstrap.server", "hadoop102:9092")
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), pro))

    kafkaDS.flatMap(_.split(" ")).map((_, 1L))
      .keyBy(t => t)
      .process(new ProcessFunction[(String, Long), (String, Long)] {
        var count: Long = 0L

        override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
          count += 1L
          out.collect((value._1, count))
        }
      }).addSink(new SinkDay05)

    env.execute()
  }

  class SinkDay05 extends RichSinkFunction[(String, Long)] {
    var conn: Connection = _
    var pst: PreparedStatement = _

    override def invoke(value: (String, Long), context: SinkFunction.Context): Unit = {
      pst.setString(1,value._1)
      pst.setLong(2,value._2)
      pst.execute()
    }

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc://hadoop102/test", "root", "123456")
      conn.prepareStatement("insert into day051 values(?,?)")
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }


}
