package com.zx.work

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName Province_Ads_Click
  * @Description TODO
  * @Author Xing
  * @Date 13 20:54
  * @Version 1.0
  */
object Province_Ads_Click {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataStream[String] = env.readTextFile("flink-test/input/AdClickLog.csv")

    dataDS.map((line: String) => {
      val strings: Array[String] = line.split(",")
      (strings(2) + "-" + strings(1), 1L)
    }).keyBy(0).sum(1).print()


    env.execute()
  }

}
