package com.atguigu.redistributingtest

import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName R4_Union
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 16:42
  * @Version 1.0
  */
object R4_Union {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1: DataStream[Int] = env.fromElements(1,2,3,4,5,6)
    val ds2: DataStream[Int] = env.fromElements(100,200,300)
    val ds3: DataStream[Int] = env.fromElements(11,22,33,44,55,66,77)

    val ds1AndDs2: DataStream[Int] = ds1.union(ds2)


    val re0DS: DataStream[Int] = ds1AndDs2.union(ds3)

    re0DS.print()

    re0DS.map(_+100000).print("re1")



    env.execute()
  }

}
