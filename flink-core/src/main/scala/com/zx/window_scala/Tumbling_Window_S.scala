package com.zx.window_scala


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @ObjectName Window_1
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Tumbling_Window_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataStream[String] = env.socketTextStream("zx101",5577)

    dataDS.keyBy(t=>t)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .sum(1)
        .print()

    dataDS.keyBy(t=>t)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)
      .print()


    env.execute()
  }
}
