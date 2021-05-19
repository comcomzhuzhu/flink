package com.zx.source


import java.util.Random

import com.zx.apitest.beans.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * @ClassName Source_Socket
  * @Description TODO
  * @Author Xing
  * @Version 1.0
  */
object Source_Socket {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = new  MyScalaSource()
    val ds: DataStream[SensorReading] = env.addSource(source)
    ds.print()
    println("main:"+source.hashCode())
    new Thread(() => {
      println("thread:" + source.hashCode())
      Thread.sleep(10000)
      source.cancel()
    }).start()
    env.execute()
  }
}

class MyScalaSource() extends SourceFunction[SensorReading] {
  @volatile var running = true
//  var flag: AtomicBoolean = new AtomicBoolean(true)

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    println("run:"+this.hashCode())
    val r: Random = new Random
    val tempMap= new mutable.HashMap[String, Double]
    for (i <- 1 to 10) {
      tempMap.put("sensor" + (i + 1), r.nextGaussian * 10)
    }

    println("running??"+running)
    while (running) {
      println("running"+this.hashCode())
      for ((key, temp) <- tempMap) {
        val newTemp: Double = tempMap.getOrElse(key,0.0) +r.nextGaussian()
        tempMap.put(key, newTemp)
        ctx.collect(new SensorReading(key, System.currentTimeMillis(), newTemp))
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    println(this.hashCode()+"---"+running)
     running = false
    println(this.hashCode()+"---"+running)
  }


}
