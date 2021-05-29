package com.zx.window_scala

import com.zx.bean.WaterSensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/** 应用场景有限  基本只有处理
  * 没有会话id时的数据 强行判断属于同一个会话的数据  -->还不如让生成数据的人加入会话id
  *
  */
object ProcessTime_SessionWindow_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("zx101",8887)

    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val strings: Array[String] = line.split(" ")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })
//         延迟小于500ms的数据会被认为是同一个会话
    waterSensorDS.keyBy(_.id)
        .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(500)))
        .sum(1)
        .print("session window")


    env.execute()
  }
}
