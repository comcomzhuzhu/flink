package com.atguigu.source
import org.apache.flink.streaming.api.scala._
/**
  * @ClassName Source_Collection
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/12 10:32
  * @Version 1.0
  */
object Source_Collection {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val list = List(1,2,3,4,5,6,7)

    val colSourceDS: DataStream[Int] = env.fromCollection(list)

    colSourceDS.print()

    val eleSource: DataStream[Any] = env.fromElements("Q","P",34)
    eleSource.print()

    val fileDS: DataStream[String] = env.readTextFile("flink-test/input/words.txt")

    fileDS.print("file")

    env.execute("from collection")
  }

}
