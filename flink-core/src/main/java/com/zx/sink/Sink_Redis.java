package com.zx.sink;

import com.alibaba.fastjson.JSON;
import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDS = env.readTextFile("flink-test/input/sensor.txt");
        SingleOutputStreamOperator<SensorReading> caseClassDS = textDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        caseClassDS.print();
//        redis 连接 config
        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost("zx101")
                .setPort(6379)
                .build();


//         连接redis
        caseClassDS.addSink(new RedisSink<>(redisConf, new MyRedisMapper()));
//
//        caseClassDS.addSink(new RedisSink<>(redisConf, new RedisMapper<SensorReading>() {
//            @Override
//            public RedisCommandDescription getCommandDescription() {
//                return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
//            }
//
//            @Override
//            public String getKeyFromData(SensorReading data) {
//                return data.getId();
//            }
//
//            @Override
//            public String getValueFromData(SensorReading data) {
//                return data.getTemperature().toString();
//            }
//        }));



        env.execute();
    }


    public static class MyRedisMapper implements RedisMapper<SensorReading> {

//         定义保存数据到redis的命令  存成hash  一个id 一个 温度  hset sensor_temp  id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(SensorReading data) {
            return JSON.toJSONString(data);
        }
    }
}
