package com.atguigu.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Sink_Redis {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDS: DataStream[String] = env.socketTextStream("hadoop102", 8881)
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()
    val sink = new RedisSink[String](config, new RedisMapper[String] {

      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "water")
      }

      override def getKeyFromData(data: String): String = {
        data
      }

      override def getValueFromData(data: String): String = {
        data
      }
    })

    socketDS.addSink(sink)
    env.execute()

  }
}
