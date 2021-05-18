package com.zx.statescala

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._

/**
  * @ObjectName State_OpList_S
  * @Description TODO
  * @Author Xing
  * @Date 19 10:15
  * @Version 1.0
  *          map 函数计算每个并行度中出现单词的次数
  */
object State_OpList_S {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val socketDS: DataStream[String] = env.socketTextStream("hadoop", 8888)

    val wordDS: DataStream[String] = socketDS.flatMap(_.split(","))

    wordDS.map(new MyRichMapFunc()).print("each parallelism")


    env.execute()
  }

  class MyRichMapFunc() extends RichMapFunction[String, (String, Long)] with CheckpointedFunction {

    var count = 0L
    var listState: ListState[Long] = _

    override def map(value: String): (String, Long) = {
      count += 1
      (value, count)
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      listState.clear()
      listState.add(count)
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      val des = new ListStateDescriptor[Long]("listState", TypeInformation.of(new TypeHint[Long]() {}))
      val list: ListState[Long] = context.getOperatorStateStore.getListState(des)

      if (context.isRestored) {
        list.get().forEach(e => count += e)
      }
    }
  }

}
