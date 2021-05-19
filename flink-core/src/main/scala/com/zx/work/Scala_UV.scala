package com.zx.work

import com.zx.bean.UserBehavior
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * @ObjectName Scala_UV
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Scala_UV {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textDS: DataStream[String] = env.readTextFile("flink-test/input/UserBehavior.csv")

    textDS.map(line => {
      val strings: Array[String] = line.split(",")
      UserBehavior(strings(0).toLong,
        strings(1).toLong,
        strings(2).toInt,
        strings(3),
        strings(4).toLong)
    }).filter(userBehavior => {
      "pv".equals(userBehavior.behavior)
    }).keyBy(_.behavior).process(new KeyedProcessFunction[String, UserBehavior, Long] {
      var hashSet = new mutable.HashSet[Long]()
      override def processElement(value: UserBehavior, ctx: KeyedProcessFunction[String, UserBehavior, Long]#Context, out: Collector[Long]): Unit = {
        hashSet.add(value.userId)
        out.collect(hashSet.size)
      }
    }).print("2")


    env.execute()
  }


}
