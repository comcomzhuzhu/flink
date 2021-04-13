package com.atguigu.work

import com.atguigu.bean.{OrderEvent, TxEvent}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.mutable

object Order_Join {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val orderDS: DataStream[OrderEvent] = env.readTextFile("flink-test/input/OrderLog.csv")
      .map(line => {
        val fields: Array[String] = line.split(",")
        OrderEvent(fields(0).toLong,
          fields(1),
          fields(2),
          fields(3).toLong)
      })

    val txDS: DataStream[TxEvent] = env.readTextFile("flink-test/input/ReceiptLog.csv")
      .map(line => {
        val fields: Array[String] = line.split(",")
        TxEvent(fields(0),
          fields(1),
          fields(2).toLong)
      })

    val coDS: ConnectedStreams[OrderEvent, TxEvent] = orderDS.connect(txDS)
    coDS.keyBy("txId","txId")
        .process(new CoProcessFunction[OrderEvent, TxEvent,String] {
          private val orderMap = new mutable.HashMap[String,OrderEvent]()
          private val txMap = new mutable.HashMap[String,TxEvent]()
          override def processElement1(value: OrderEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
            if (txMap.contains(value.txId)){
              out.collect("订单"+value.orderId+"对账成功")
              txMap.remove(value.txId)
            }else{
              orderMap.put(value.txId,value)
            }
          }

          override def processElement2(value: TxEvent, ctx: CoProcessFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]): Unit = {
            if(orderMap.contains(value.txId)){
              out.collect("订单"+ orderMap(value.txId).orderId+"对账成功")
            }else{
              txMap.put(value.txId,value)
            }
          }
        }).print()

    env.execute()
  }

}
