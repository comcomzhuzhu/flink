package com.zx.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @ObjectName ProcessTimeTumbingWindow_S
  * @Description TODO
  * @Author Xing
  * 14 19:00
  * @Version 1.0
  */
object ProcessTime_TumbingWindow_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8887)

    val keyedStream: KeyedStream[(String, Long), String] = socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        strings.foreach(t => out.collect((t, 1L)))
      }
    }).keyBy(t => t._1)


//                offset  和 左闭右开 如果开窗口是days(1) 要偏移量-8
    /**
      * Gets the largest timestamp that still belongs to this window.
      *
      * <p>This timestamp is identical to {@code getEnd() - 1}.
      *
      * @return The largest timestamp that still belongs to this window.
      *
      * @see #getEnd()
      */
//    @Override
//    public long maxTimestamp() {
//      return end - 1;
//    }
    val windowDS: WindowedStream[(String, Long), String, TimeWindow] = keyedStream.window(TumblingProcessingTimeWindows.of(Time.milliseconds(5000)))
      .allowedLateness(Time.milliseconds(50))


    keyedStream.window(SlidingProcessingTimeWindows.of(Time.milliseconds(5000),Time.milliseconds(50)))

    windowDS.sum(1).print("window")


    env.execute()

  }
}
