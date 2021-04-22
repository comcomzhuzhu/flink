package com.atguigu.day5work

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Work2
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/14 8:59
  * @Version 1.0
  */
object Work2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 1589)
    socketDS.flatMap(_.split(",")).keyBy(t => t)
      .process(new KeyedProcessFunction[String, String, String] {
        var set = new mutable.HashSet[String]()
        override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
          if (!set.contains(value))
            out.collect(value)
          set.add(value)
        }
      }).print("word  dot in the Set")

    env.execute()
  }
}
