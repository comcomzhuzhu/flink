package com.zx.workcount

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable

object Work1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val pro = new Properties()
    pro.put("bootstrap.server", "zx101:9092")
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("first", new SimpleStringSchema(), pro))

    env.socketTextStream("zx101", 1589)
      .flatMap(_.split(" ")).map((_, 1L))
      .keyBy(_._1)
      //      .reduce(new ReduceFunction[(String, Long)] {
      //      override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
      //        (value1._1,value1._2+1)
      //      }
      //    }).print("xx")


      //           不能使用一个  val count = 0L 因为这个count是每个并行度共享
      .process(new ProcessFunction[(String, Long), mutable.HashMap[String, Long]] {
      val map = new mutable.HashMap[String, Long]()

      override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), mutable.HashMap[String, Long]]#Context, out: Collector[mutable.HashMap[String, Long]]): Unit = {
        map(value._1) = map.getOrElse(value._1, 0l) + 1L
        out.collect(map)
      }
    }).print("test")


    env.execute()
  }

  class Sink5 extends RichSinkFunction[(String, Long)] {
    var conn: Connection = _
    var pst: PreparedStatement = _

    override def invoke(value: (String, Long), context: SinkFunction.Context): Unit = {
      pst.setString(1, value._1)
      pst.setLong(2, value._2)
      pst.execute()
    }

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc://zx101/test", "root", "123456")
      conn.prepareStatement("insert into  values(?,?)")
    }

    override def close(): Unit = {
      pst.close()
      conn.close()
    }
  }


}
