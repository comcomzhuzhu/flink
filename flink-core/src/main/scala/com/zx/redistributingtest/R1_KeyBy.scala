package com.zx.redistributingtest

import com.zx.bean.WaterSensor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
/**
  * keyBy
  */
object R1_KeyBy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val valueDS: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    //    TODO  使用下标的方式 适用于元组 keyBy(0)
    valueDS.map((_, "Q")).keyBy(0).print("key by 0")

    val textDS: DataStream[String] = env.readTextFile("flink-test/input/water.txt")
    val caseClassDS: DataStream[WaterSensor] = textDS.map((line: String) => {
      val strings: Array[String] = line.split(",")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })

//    caseClassDS.setParallelism(1)
    //    TODO  使用字段的方式 适用于POJO
    val keyDS: KeyedStream[WaterSensor, Tuple] = caseClassDS.keyBy("id")
//   TODO  keyedStream  cannot set the parallelism
    keyDS.print("keDS******")
    val xx: DataStream[(WaterSensor, Int)] = keyDS.map((_,1))

    xx.print("xxxxxxxx")
    //    TODO  实现key选择器
    caseClassDS.setParallelism(1)
//    TODO keyed DS 不能设置并行度 并行度和之前一样
    val keyedS: KeyedStream[WaterSensor, Boolean] = caseClassDS.keyBy(_.vc > 20)
    keyedS.print("key by keySelector")
    env.execute()
  }

}
