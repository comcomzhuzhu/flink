package com.atguigu.transform

import com.atguigu.bean.WaterSensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object T3_RichMap {
//      生命周期方法        open 和close方法
//
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val sourceDS: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8899)

    val textDS: DataStream[String] = env.readTextFile("flink-test/input/water.txt")


    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(new RichMapFunction[String, WaterSensor] {
      override def map(value: String): WaterSensor = {
        val strings: Array[String] = value.split(" ")
        WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
      }

      override def open(parameters: Configuration): Unit = {
        // 创建连接
      }

      override def close(): Unit = {
        // 关闭连接
      }
    })


//    TODO 如果是readTextFile    每个并行度close调用两次

    val textMap: DataStream[WaterSensor] = textDS.map(new RichMapFunction[String, WaterSensor] {
      override def map(value: String): WaterSensor = {
        val strings: Array[String] = value.split(",")
        println(getRuntimeContext.getTaskName+"--"+getRuntimeContext.getTaskNameWithSubtasks)
        WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
      }

      override def open(parameters: Configuration): Unit = {
        // 创建连接
      }

      override def close(): Unit = {
        // 关闭连接
        println("close")
      }
    })

    textMap.print()

    env.execute()
  }
}
