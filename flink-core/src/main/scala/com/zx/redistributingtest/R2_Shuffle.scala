package com.zx.redistributingtest

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName R2_Shuffle
  * @Description TODO
  * @Author Xing
  * @Date 12 16:25
  * @Version 1.0
  */
object R2_Shuffle {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromElements(1,2,3,4)

    ds.shuffle

    env.execute()
  }
}
