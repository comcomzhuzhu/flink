package com.zx.window_f_s

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Window_F_ReduceF
  * @Description TODO
  * @Author Xing
  * @Date 16 18:00
  * @Version 1.0
  */
object Window_F_ReduceF {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketTS: DataStream[String] = env.socketTextStream("hadoop102", 5577)

    val wordDS: DataStream[(String, Long)] = socketTS.flatMap(line => {
      val list = mutable.ListBuffer[(String, Long)]()
      line.split(",").foreach(s => list += ((s, 1L)))
      list
    })

    wordDS.keyBy(_._1).reduce((v1, v2) => (v1._1, v1._2 + v2._2))


    wordDS.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      }).print("reduce")

    wordDS.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
      .reduce { (v1, v2) => {
        (v1._1, v1._2 + v2._2)
      }}

    wordDS.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
        .apply(new WindowFunction[(String,Long),(Long,String,Long),String,TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(Long, String, Long)]): Unit = {
            val start: Long = window.getStart
            val size: Int = input.toList.size
            out.collect(start,key,size)
          }
        })


    env.execute()
  }

}
