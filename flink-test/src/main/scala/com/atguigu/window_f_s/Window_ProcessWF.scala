package com.atguigu.window_f_s

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Window_ProcessWF
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/16 18:52
  * @Version 1.0
  */
object Window_ProcessWF {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketTS: DataStream[String] = env.socketTextStream("hadoop102", 5577)

    val wordDS: DataStream[(String, Long)] = socketTS.flatMap(line => {
      val list = mutable.ListBuffer[(String, Long)]()
      line.split(",").foreach(s => list += ((s, 1L)))
      list
    })


    wordDS.keyBy(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[(String, Long), (Long, String, Long), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(Long, String, Long)]): Unit = {
          val size: Int = elements.size
          val start: Long = context.window.getStart
          out.collect(start, key, size)
        }
      }).print("process wf")


    env.execute()
  }
}
