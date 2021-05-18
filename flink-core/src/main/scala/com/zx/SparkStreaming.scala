package com.zx

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming {
  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext(new SparkConf(),Seconds(3))

    ssc.checkpoint("")

//    StreamingContext.getActiveOrCreate()
//    spark 从挂掉的一刻开始计算 会一次性提交
//    从挂掉的那一刻 到  重新启动应用  时间/seconds(5) 个 任务
//    如果挂掉一小时  一次性提交 3600S/5S  = 720个任务

    ssc.start()
    ssc.awaitTermination()

  }

}
