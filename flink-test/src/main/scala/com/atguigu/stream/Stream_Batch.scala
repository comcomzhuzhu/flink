package com.atguigu.stream

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._

/**
  * @ClassName Stream_Batch
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 10:13
  * @Version 1.0
  */
object Stream_Batch {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
//    TODO 真正的批模式
//    TODO 之前的是流式处理  有界流

    val dataDS: DataStream[String] = env.readTextFile("flink-test/input/words.txt")

    val value: DataStream[String] = dataDS.flatMap(_.split(" "))

    val map2: DataStream[(String, Int)] = value.map((_,1))

    map2.keyBy(0).sum(1).print("QQ")


    dataDS.print()
    env.execute()
  }

}
