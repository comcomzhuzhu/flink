package com.zx.partitioner

import com.zx.bean.WaterSensor
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName rebalance
  * @Description TODO
  * @Author Xing
  * 13 9:15
  * @Version 1.0
  */
object rebalance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//      env.setStreamTimeCharacteristic(TtlTimeCharacteristic.)
    val textDS: DataStream[String] = env.readTextFile("flink-test/input/water.txt")

    val caseClassDS: DataStream[WaterSensor] = textDS.map(line => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })




  }
}
