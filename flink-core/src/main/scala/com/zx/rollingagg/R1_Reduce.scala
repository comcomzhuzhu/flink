package com.zx.rollingagg

import com.zx.bean.WaterSensor
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName R1_Reduce
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object R1_Reduce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("zx101",9944)

    val caseClassDS: DataStream[WaterSensor] = socketDS.map {
      line: String => {
        val strings: Array[String] = line.split(" ")
        WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
      }
    }

    caseClassDS.keyBy((_: WaterSensor).id)
        .reduce((t1: WaterSensor, t2: WaterSensor)=>{
          WaterSensor(t1.id, System.currentTimeMillis(),t1.vc+t2.vc)
        }).print()

    env.execute()
  }
}
