package com.zx.transform

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName T1_map
  * @Description TODO
  * @Author Xing
  * 12 11:41
  * @Version 1.0
  */
object T1_map {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceDS: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    sourceDS.print()

    env.execute()

  }
}
