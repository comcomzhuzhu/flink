package com.atguigu.wordcount

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
/**
  * @ObjectName Test
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/8 23:41
  * @Version 1.0
  */
object Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataSS: DataStream[String] = env.readTextFile(" ")

    dataSS.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    env.execute()
  }
}
