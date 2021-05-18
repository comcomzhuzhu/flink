package com.zx.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

/**
  * @ClassName SourceCancel
  * @Description TODO
  * @Author Xing
  * @Date 11 15:55
  * @Version 1.0
  */
object SourceCancel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source= new MyTestSource()
    val ds: DataStream[String] = env.addSource(source)
    ds.print()
    println("main:"+source.hashCode())
    new Thread(() => {
      println("thread:"+source.hashCode())
      Thread.sleep(5000)
      source.cancel()
    }).start()
    env.execute()
  }
}

class MyTestSource() extends SourceFunction[String]{
  @volatile var running = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    println("run:"+this.hashCode())
    while (running){
      println("while true"+this.hashCode()+"true")
      println(this.running)
      Thread.sleep(1000)
      println("************************")
    }
  }

  override def cancel(): Unit = {
    running=false
    println(this.hashCode()+"cancel"+running)
    println(this.running)
  }
}
