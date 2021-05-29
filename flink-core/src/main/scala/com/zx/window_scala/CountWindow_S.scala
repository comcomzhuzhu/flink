package com.zx.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ClassName CountWindow_S
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object CountWindow_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("zx101", 8887)

    val keyedStream: KeyedStream[(String, Long), String] = socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        strings.foreach(t => out.collect((t, 1L)))
      }
    }).keyBy(t => t._1)


    socketDS.flatMap(line => {
      val strings: Array[String] = line.split(" ")
      val buffer = new ListBuffer[(String, Long)]()
      strings.foreach(s => {buffer += Tuple2(s, 1L)})
      buffer
    }).keyBy(t=>t._1)


    //     keyBy 了 对每一个KEY 进行了开窗 每个key要输入五个相同的数据才有输出
    keyedStream.countWindow(5)
      .sum(1)
      .print("count window 5")

    //       输入两个相同的数据就输出了
    //      因为每两个数据就滑动了 滑动步长决定输出  再向前补齐窗口 没有数据 所以第一次滑动输出2个数据
    //      第二次滑动输出4个数据 之后都输出五个数据
    keyedStream.countWindow(5, 2)
      .sum(1)
    println("count window 5 2")


    env.execute()
  }

}
