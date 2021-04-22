package com.atguigu.window_scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * TODO 对于迟到和乱序的数据
  *   需要事件时间语义waterMark  需要allowedLateness  需要侧输出流 三重保障
  *   对与乱序数据 准时输出窗口结果 再在allowedLateness时间内 更新结果
  *   之后做批流合并处理 得到准确的结果
  */
object WindowOtherAPI {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8887)

  val keyedStream: KeyedStream[(String, Long), String] = socketDS.flatMap(new FlatMapFunction[String, (String, Long)] {
    override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
      val strings: Array[String] = value.split(" ")
      strings.foreach(t => out.collect((t, 1L)))
    }
  }).keyBy(t => t._1)
  private val reDS: DataStream[(String, Long)] = keyedStream.window(TumblingEventTimeWindows.of(Time.milliseconds(5000)))
    //          .trigger()
    //          .evictor()
    //    TODO  允许延迟的时间   如果设置了允许迟到的时间   事件时间 watermark
    //     到时间了 输出结果但是不关闭窗口 如果有迟到的数据来 在允许迟到的时间间隔上
    //     更新之前的结果  到了窗口时间+延迟时间 窗口会关闭
    //     Setting an allowed lateness is only valid for event-time windows.
    .allowedLateness(Time.milliseconds(100))
    //       窗口关闭之后还有属于之前窗口的数据 扔入侧输出流
    .sum(1)
//         虽然在窗口时间内 输出了结果 但是在100ms内还会更新结果

//        最终保底方案 如果还有属于之前那个窗口的数据 在侧输出流中
        reDS.getSideOutput(new OutputTag[(String,Long)]("late"))
//         之后批处理  处理两条流的数据  实现了两套架构的结果
  //         流处理快速输出结果 +侧输出流 批处理


  env.execute()
}
