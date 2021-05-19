package com.zx.statescala

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * @ObjectName State_OpBroadCast
  * @Description TODO
  * @Author Xing
  * 19 10:50
  * @Version 1.0
  */
object State_OpBroadCast {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    读取数据创建主流  读取数据创建配置流
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 9999)
    val channelDS: DataStream[String] = env.socketTextStream("hadoop102", 9992)
    //    将配置流广播

    val mapDes = new MapStateDescriptor[String, String]("channel",
      TypeInformation.of(new TypeHint[String]() {}),
      TypeInformation.of(new TypeHint[String]() {})
    )

    val broDS: BroadcastStream[String] = channelDS.broadcast(mapDes)
    //       将两个流进行连接
    val connDS: BroadcastConnectedStream[String, String] = socketDS.connect(broDS)

    connDS.process(new BroadcastProcessFunction[String, String, String] {
      override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {

        val readBroState: ReadOnlyBroadcastState[String, String] = ctx.getBroadcastState(mapDes)

        val channel: String = readBroState.get("channel")
        //       根据广播状态处理数据
        if ("1".equals(channel)) {
          out.collect("1" + ":" + value)
        } else {
          out.collect("other" + ":" + value)
        }


      }

      override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        //        获取广播状态 并存入数据
        val braState: BroadcastState[String, String] = ctx.getBroadcastState(mapDes)
        ctx.getBroadcastState(mapDes).put("channel", value)

      }
    })


    env.execute()
  }
}
