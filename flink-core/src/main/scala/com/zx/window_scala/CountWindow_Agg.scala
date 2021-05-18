package com.zx.window_scala

import com.zx.bean.WaterSensor
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName CountWindow_Agg
  * @Description TODO
  * @Author Xing
  * @Date 14 18:12
  * @Version 1.0
  */
object CountWindow_Agg {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",8887)

    val waterSensorDS: DataStream[WaterSensor] = socketDS.map(line => {
      val strings: Array[String] = line.split(" ")
      WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
    })
    waterSensorDS.keyBy(_.id)
//TODO    每隔两个数滑动一次 输出一次 包括向前补全窗口
      .countWindow(10,2)
        .aggregate(new AggregateFunction[WaterSensor,(Double,Int),Double] {
//          累加器的创建方法
          override def createAccumulator(): (Double, Int) = {
            (0.0,0)
          }
//          累加器的对单个值的累加效果
          override def add(value: WaterSensor, accumulator: (Double, Int)): (Double, Int) = {
            (accumulator._1+value.vc,accumulator._2+1)
          }
//            累加器的输出方法
          override def getResult(accumulator: (Double, Int)): Double = {
            accumulator._1/accumulator._2
          }
//            累加器的合并方法
          override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) = {
            (a._1+b._1,a._2+b._2)
          }
        }).print("avg temp")



    env.execute()
  }

}
