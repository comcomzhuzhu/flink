package com.atguigu.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * @ObjectName Sink_Es
  * @Description TODO
  * @Author Xing
  * @Date 2021/4/13 10:24
  * @Version 1.0
  */
object Sink_Es {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8877)

    val httpHosts = new java.util.ArrayList[HttpHost]

    httpHosts.add(new HttpHost("hadoop102", 9200))

    new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String] {

      val json = new java.util.HashMap[String, String]

      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        json.put("data", t)


        val rqst: IndexRequest = Requests.indexRequest
          .index("4-13")
          .`type`("_doc")
          .source(json)

        requestIndexer.add(rqst)
      }
    })

    socketDS.print()
    env.execute()

  }

}
