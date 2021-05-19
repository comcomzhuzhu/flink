package com.zx.ceptest

import java.time.Duration

import com.zx.bean.LoginE
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ObjectName CEP_LoginFail
  * @Description TODO
  * @Author Xing
  * 20 20:14
  * @Version 1.0
  */
object CEP_LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val textFDS: DataStream[String] = env.readTextFile("flink-test/input/LoginLog.csv")

    val caseClassDS: DataStream[LoginE] = textFDS.map(line => {
      val strings: Array[String] = line.split(",")
      LoginE(strings(0), strings(1), strings(2), strings(3).toLong)
    }).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[LoginE](Duration.ofMillis(2000))
      .withTimestampAssigner(new SerializableTimestampAssigner[LoginE] {
        override def extractTimestamp(element: LoginE, recordTimestamp: Long): Long = {
          element.ts * 1000L
        }
      }))

    val keyedDS: KeyedStream[LoginE, String] = caseClassDS.keyBy(_.id)



    val pattern: Pattern[LoginE, LoginE] =
      Pattern.begin[LoginE]("start").where(new SimpleCondition[LoginE] {
        override def filter(value: LoginE): Boolean = {
          value.state.equals("fail")
        }
      })
        .times(2)
        .consecutive()
        .within(Time.seconds(2))









    env.execute()
  }

}
