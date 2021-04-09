package com.atguigu.wordcount

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @ObjectName WordCount
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/8 22:43
  * @Version 1.0
  */

object WordCount {
  def main(args: Array[String]) {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("hadoop102", 9999)
    val counts: DataStream[(String, Int)] = text.flatMap { _.split("\t").filter { _.nonEmpty } }
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()
    env.execute("Window Stream WordCount")
  }
}
