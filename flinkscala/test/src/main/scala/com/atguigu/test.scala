package com.atguigu

/**
  * @ClassName test
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/8 23:50
  * @Version 1.0
  */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object test {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }
}