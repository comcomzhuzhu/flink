package com.zx.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * @ObjectName GlobalWindow_S
  * @Description TODO
  * @Author Xing
  * @Date 14 18:29
  * @Version 1.0
  */
object GlobalWindow_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8887)

    val keyedStream: KeyedStream[(String, Long), String] = socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        strings.foreach(t => out.collect((t, 1L)))
      }
    }).keyBy(t => t._1)

    keyedStream.window(GlobalWindows.create())
        .trigger(new Trigger[(String,Long),GlobalWindow] {
          override def onElement(element: (String, Long), timestamp: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.FIRE_AND_PURGE
          }

          override def onProcessingTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = {
            TriggerResult.CONTINUE
          }

          override def onEventTime(time: Long, window: GlobalWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

          override def clear(window: GlobalWindow, ctx: Trigger.TriggerContext): Unit = {}
        })
        .sum(1)
        .print("global window sum")




    env.execute()
  }

}
