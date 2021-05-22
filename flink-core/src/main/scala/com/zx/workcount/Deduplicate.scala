package com.zx.workcount

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Deduplicate
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Deduplicate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 1589)
    socketDS.flatMap(_.split(",")).keyBy(t => t)
      .process(new KeyedProcessFunction[String, String, String] {
        var set = new mutable.HashSet[String]()

        override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
          if (!set.contains(value)) {
            out.collect(value)
          }
          set.add(value)
        }
      }).print("word  dont in the Set")

    env.execute()
  }
}
