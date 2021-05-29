package com.zx.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @ObjectName ProcessWindowF_S
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object ProcessWindowF_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("zx101", 8887)

    val keyedStream: KeyedStream[(String, Long), String] = socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
      override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
        val strings: Array[String] = value.split(" ")
        strings.foreach(t => out.collect((t, 1L)))
      }
    }).keyBy(t => t._1)

    keyedStream.window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
      //     IN OUT KEY WINDOW
      .apply(new WindowFunction[(String, Long), Long, String, TimeWindow] {
      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[Long]): Unit = {
        var count = 0L
        input.foreach(count += _._2)
        out.collect(count)
      }
    }).print("zz")

    env.execute()
  }
}
