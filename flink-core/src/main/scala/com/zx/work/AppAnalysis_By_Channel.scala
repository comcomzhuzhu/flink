package com.zx.work

import com.zx.bean.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
  * @ObjectName AppAnalysis_By_Channel
  * @Description TODO
  * @Author Xing
  * 13 20:20
  * @Version 1.0
  */
object AppAnalysis_By_Channel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataStream[MarketingUserBehavior] = env.addSource(new ScalaMarketDataSource)

//    dataDS.keyBy(new JavaKeySelector[MarketingUserBehavior,String](
//      caseClass =>{
//        caseClass.channel+caseClass.behavior
//      }))
//        .process(new ProcessFunction[MarketingUserBehavior,String] {
//          var count = 0L
//          override def processElement(value: MarketingUserBehavior, ctx: ProcessFunction[MarketingUserBehavior, String]#Context, out: Collector[String]): Unit = {
//            count+=1
//            out.collect(value.channel+"-"+value.behavior+" "+count)
//          }
//        }).print("")

    dataDS.map(behavior =>(behavior.channel+"_"+behavior.behavior,1L))
        .keyBy(0).sum(1).print("xx")


    dataDS.map(behavior=>(behavior.behavior,1L))
        .keyBy(0).sum(1)


    env.execute()
  }

  class ScalaMarketDataSource extends RichSourceFunction[MarketingUserBehavior] {
    @volatile var running = true
    private val random = new Random()
    private val channels = List("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo")
    private val behaviors = List("download", "install", "update", "uninstall")

    override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
      while (running) {
        val marketingUserBehavior = MarketingUserBehavior(
          random.nextInt(1000000).toLong,
          behaviors(random.nextInt(behaviors.size)),
          channels(random.nextInt(channels.size)),
          System.currentTimeMillis()
        )

        ctx.collect(marketingUserBehavior)
        Thread.sleep(2000)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}
