package com.zx.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * @ObjectName Source_Kafka
  * @Description TODO
  * @Author Xing
  * 12 10:39
  * @Version 1.0
  */
object Source_Kafka {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val pro = new Properties()
    pro.setProperty("bootstrap.servers","hadoop102:9092")
    pro.setProperty("group.id","scala")

    val kafkaSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("first",new SimpleStringSchema(),pro))
    kafkaSource.setParallelism(3)


    kafkaSource.print()

    env.execute()
  }
}
