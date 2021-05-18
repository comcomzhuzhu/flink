package com.zx.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * @ObjectName Window_All_S
  * @Description TODO
  * @Author Xing
  * @Date 14 19:23
  * @Version 1.0
  */
object Window_All_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8887)

    val dataDS= socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        strings.foreach(t => out.collect((t, 1L)))
      }
    })

//    window All 放入了一个窗口中
    dataDS.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(5000)))
        .sum(1).print()


    env.execute()
  }

}
