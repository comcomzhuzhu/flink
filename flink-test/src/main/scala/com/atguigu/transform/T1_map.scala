package com.atguigu.transform

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName T1_map
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 11:41
  * @Version 1.0
  */
object T1_map {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDS: DataStream[Int] = env.fromElements(1,2,3,4,5)

    val mapDS: DataStream[Int] = sourceDS.map(_+1)

    mapDS.print()

    env.execute()

  }
}
