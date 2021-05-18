package com.zx.source

import com.zx.bean.WaterSensor
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * @ClassName Source_Custom
  * @Description TODO
  * @Author Xing
  * @Date 12 11:24
  * @Version 1.0
  */
object Source_Custom {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    TODO 自定义接口
    val mySource: DataStream[WaterSensor] = env.addSource(new MyCustomSource)

    mySource.print()

    env.execute()
  }

  class  MyCustomSource extends  SourceFunction[WaterSensor]{
    @volatile var running = true

    override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {
      val random = new Random()
      while (running){
        Thread.sleep(1000)
        ctx.collect(WaterSensor("id:" + random.nextInt(100), System.currentTimeMillis(), random.nextInt(20)))
      }
    }

    override def cancel(): Unit = {
      running =false
    }
  }

  class  PSource extends  RichParallelSourceFunction[WaterSensor]{
    override def run(ctx: SourceFunction.SourceContext[WaterSensor]): Unit = {

    }

    override def cancel(): Unit = {

    }
  }
}

