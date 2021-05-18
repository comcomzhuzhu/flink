package com.zx.scalawatermark

import java.time.Duration

import com.zx.bean.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object OrderedWaterMark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",8544)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val caseClassDS: DataStream[WaterSensor] = socketDS.map((line: String) => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })


//     start so that our lowest watermark would be Long.MIN_VALUE.
//     避免数据溢出 this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1
    val waterMDS: DataStream[WaterSensor] = caseClassDS.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = {
          element.ts * 1000
        }
      }))

    val keyedDS: KeyedStream[WaterSensor, String] = waterMDS.keyBy((_: WaterSensor).id)
    keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .allowedLateness(Time.seconds(2))
        .sideOutputLateData(new OutputTag[WaterSensor]("late"){})
        .sum("vc")
        .print("window")





    env.execute()
  }

}
