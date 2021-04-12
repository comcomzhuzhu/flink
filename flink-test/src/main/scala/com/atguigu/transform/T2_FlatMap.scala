package com.atguigu.transform

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @ObjectName T2_FlatMap
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 11:48
  * @Version 1.0
  */
object T2_FlatMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceDS: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    val re0DS: DataStream[Int] = sourceDS.flatMap(new MyFlatMap)

    val re1DS: DataStream[Any] = sourceDS.flatMap((value: Int, out: Collector[Any]) => {
      out.collect(value + 12000)
      out.collect(value + "QQ")
    })

    re0DS.print()
    re1DS.print("flatMap")

    env.execute()
  }


  class MyFlatMap extends  FlatMapFunction[Int,Int]{
    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      out.collect(value+1)
      out.collect(value+1000)
    }
  }
}
