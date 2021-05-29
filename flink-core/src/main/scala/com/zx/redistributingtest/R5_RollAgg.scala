package com.zx.redistributingtest

import com.zx.bean.WaterSensor
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object R5_RollAgg {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("zx101",9977)

    val caseClassDS: DataStream[WaterSensor] = socketDS.map(new MapFunction[String, WaterSensor] {
      override def map(value: String): WaterSensor = {
        val fields: Array[String] = value.split(",")
        WaterSensor(fields(0), fields(1).toLong, fields(2).toInt)
      }
    })

    val keyedS: KeyedStream[WaterSensor, Int] = caseClassDS.keyBy(_.vc)

//    TODO  max 仅仅vc字段变成了最大值，其他值都是第一条数据的 默认first+true
    keyedS.max("vc").print()

//     TODO  maxBy vc字段变成了最大值，其他值也是最大值的
//      如果有两条数据都是最大值       默认first + true 使用第一条的
//       scala没法传参
    keyedS.maxBy("vc")


    env.execute()
  }

}
