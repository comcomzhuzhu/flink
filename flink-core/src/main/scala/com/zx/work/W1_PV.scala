package com.zx.work

import com.zx.bean.UserBehavior
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @ClassName W1
  * @Description TODO
  * @Author Xing
  * 13 19:57
  * @Version 1.0
  */
object W1_PV {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textDS: DataStream[String] = env.readTextFile("flink-test/input/UserBehavior.csv")

    textDS.filter(line=>{
      val strings: Array[String] = line.split(",")
      "pv".equals(strings(3))
    }).map(line=>{
      val strings: Array[String] = line.split(",")
      (strings(3),1L)
    }).keyBy(new JavaKeySelector[(String,Long),String](
      _._1)).sum(1).print("1")


    textDS.map(line=>{
      val strings: Array[String] = line.split(",")
      UserBehavior(strings(0).toLong,
        strings(1).toLong,
        strings(2).toInt,
        strings(3),
        strings(4).toLong)
    }).filter(userBehavior=>{
      "pv".equals(userBehavior.behavior)
    }).keyBy(_.behavior).process(new KeyedProcessFunction[String,UserBehavior,Long] {
      var count:Long=0
      override def processElement(value: UserBehavior, ctx: KeyedProcessFunction[String, UserBehavior, Long]#Context, out: Collector[Long]): Unit = {
        count+=1
        out.collect(count)
      }
    }).print("2")

    env.execute()
  }

}
