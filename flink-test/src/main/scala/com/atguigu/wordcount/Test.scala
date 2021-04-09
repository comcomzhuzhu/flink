package com.atguigu.wordcount

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * @ObjectName Test
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/8 23:41
  * @Version 1.0
  */
object Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val unit: DataStreamSource[String] = env.readTextFile("")

    unit.map(x=>x.toInt).writeAsText("")
    println()
  }
}
