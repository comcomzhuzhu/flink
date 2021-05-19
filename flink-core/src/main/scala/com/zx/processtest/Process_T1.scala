package com.zx.processtest

import com.zx.bean.WaterSensor
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @ObjectName Process_T1
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Process_T1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102",9944)

    val caseClassDS: DataStream[WaterSensor] = socketDS.map {
      line: String => {
        val strings: Array[String] = line.split(" ")
        WaterSensor(strings(0), strings(1).toLong, strings(2).toInt)
      }
    }


    val proDS: DataStream[String] = caseClassDS.process((value: WaterSensor, ctx: ProcessFunction[WaterSensor, String]#Context, out: Collector[String]) => {
      out.collect(value.id + "-" + value.ts)
    })

    val keyedDS: KeyedStream[String, String] = proDS.keyBy(r=>r.split("-")(0))

    keyedDS.process(new ProcessFunction[String,String] {
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        out.collect(value.split("-")(1)+"10000")
      }
    }).print("key by 之后")




    caseClassDS.keyBy(_.id)

    env.execute()
  }
}
