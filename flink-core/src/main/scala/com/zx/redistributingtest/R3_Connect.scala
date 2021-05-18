package com.zx.redistributingtest

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName R3_Connect
  * @Description TODO
  * @Author Xing
  * @Date 12 16:35
  * @Version 1.0
  */
object R3_Connect {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[Int] = env.fromElements(1,2,3,4)

    ds.shuffle

    val ds2: DataStream[String] = env.fromElements("21","Q","A","b")


    val coDS: ConnectedStreams[Int, String] = ds.connect(ds2)


    val re0DS: DataStream[String] = coDS.map(new CoMapFunction[Int, String, String] {
      override def map1(value: Int): String = {
        value.toString
      }
      override def map2(value: String): String = {
        value
      }
    })

    re0DS.print()
    env.execute()
  }

}
