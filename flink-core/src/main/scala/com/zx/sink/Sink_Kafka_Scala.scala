package com.zx.sink

import com.zx.bean.WaterSensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
  * @ObjectName Sink_Kafka
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Sink_Kafka_Scala {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val textDS: DataStream[String] = env.readTextFile("flink-test/input/water.txt")

    val caseClassDS: DataStream[WaterSensor] = textDS.map(line => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })

    val lineDS: DataStream[String] = caseClassDS.map(caseClass =>caseClass.toString)

    val sink = new FlinkKafkaProducer[String]("zx101:9092","first",new SimpleStringSchema())

    lineDS.addSink(sink)

    env.execute()
  }

}
