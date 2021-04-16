package com.atguigu.window_f_s

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Window_F_AggF
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/16 18:34
  * @Version 1.0
  */
object Window_F_AggF {
  def main(args: Array[String]): Unit = {

      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      val socketTS: DataStream[String] = env.socketTextStream("hadoop102", 5577)

      val wordDS: DataStream[(String, Long)] = socketTS.flatMap(line => {
        val list = mutable.ListBuffer[(String, Long)]()
        line.split(",").foreach(s => list += ((s, 1L)))
        list
      })

    wordDS.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(new MyAGG, new MyProcessWF)

      .print("agg window")
    env.execute()
  }

  case class MyAGG() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  case class MyProcessWF() extends ProcessWindowFunction[Long, (Long, String, Long), String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(Long, String, Long)]): Unit = {
      val start: Long = context.window.getStart
      val size: Int = elements.size
      out.collect(start, key, size)
    }
  }

}