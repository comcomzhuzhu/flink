package com.atguigu.wc

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows

/**
  * @ClassName WordCount
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/9 20:10
  * @Version 1.0
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textDS: DataStream[String] = env.socketTextStream("hadoop102",9999)

    val result: DataStream[(String, Int)] = textDS.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)
    result.print()

    env.execute("Window Stream WordCount")

  }

}
