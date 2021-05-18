package com.zx.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object Sink_Es_Scala{
//    TODO 无界流写入ES 需要设置 批次执行参数
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8877)

    val httpHosts = new java.util.ArrayList[HttpHost]

    httpHosts.add(new HttpHost("hadoop102", 9200))
    httpHosts.add(new HttpHost("hadoop103", 9200))
    httpHosts.add(new HttpHost("hadoop104", 9200))

    val sinkBuilder = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String]() {
      val json = new java.util.HashMap[String, String]

      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        json.put("data", t)

        val rqst: IndexRequest = Requests.indexRequest
          .index("es413")
          .`type`("_doc")
          .source(json)
        requestIndexer.add(rqst)
      }
    })

    sinkBuilder.setBulkFlushMaxActions(1)

    socketDS.addSink(sinkBuilder.build())
    socketDS.print()
    env.execute()

  }

}
