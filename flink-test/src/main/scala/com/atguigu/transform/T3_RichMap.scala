package com.atguigu.transform

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName T3_RichMap
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 11:57
  * @Version 1.0
  */
object T3_RichMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceDS: DataStream[Int] = env.fromElements(1, 2, 3, 4)


    sourceDS.map(new RichMapFunction[Int,Any] {
      override def map(value: Int): Any = {

      }

      override def open(parameters: Configuration): Unit = super.open(parameters)

      override def close(): Unit = super.close()
    })

    env.execute()
  }

}
