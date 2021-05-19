package com.zx.wc

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName WordCount
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val lineDS: DataStream[String] = env.socketTextStream("hadoop102",9999)
    lineDS.flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()
    env.execute("scala UNBOUNDED stream")
  }

}
