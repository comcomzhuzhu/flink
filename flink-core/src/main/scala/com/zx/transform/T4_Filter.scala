package com.zx.transform

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName T4_Filter
  * @Description TODO
  * @Author Xing
  * @Date 12 14:28
  * @Version 1.0
  */
object T4_Filter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val value: DataStream[Int] = env.fromElements(1,2,3,4,5,6)

    value.filter(_%2==0).print()




    env.execute()
  }

}
