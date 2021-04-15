package com.atguigu.scalawatermark

import java.time.Duration

import com.atguigu.bean.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName OrderedWaterMark
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/14 0:12
  * @Version 1.0
  */
object OrderedWaterMark {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",8544)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val caseClassDS: DataStream[WaterSensor] = socketDS.map(line => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })


//     start so that our lowest watermark would be Long.MIN_VALUE.
//     避免数据溢出 this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
        .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = {
            element.ts *1000
          }
        })


    env.execute()
  }

}
