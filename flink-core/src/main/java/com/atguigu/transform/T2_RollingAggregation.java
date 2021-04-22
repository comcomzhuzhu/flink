package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
   return KeyGroupRangeAssignment.assignKeyToParallelOperator
   (key, maxParallelism, numberOfChannels);
 */
public class T2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> mapDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fileds = value.split(",");
            return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = mapDS.keyBy("id");


        mapDS.keyBy(date->date.getId());

        KeyedStream<SensorReading, String> keyedStream1 = mapDS.keyBy(SensorReading::getId);

        SingleOutputStreamOperator<SensorReading> maxT = keyedStream.max("temperature");
//       TODO 只改变了一个字段 其他的字段还是来自第一条数据 滚动聚合
        maxT.print();

//        TODO 所有字段都变成了 最大值对应的 属性了  如果有相同的最大值的两条数据 默认使用first的其他属性
        SingleOutputStreamOperator<SensorReading> temperature = keyedStream.maxBy("temperature");

//       TODO   可以修改为有最大值相同的两条数据, 使用第二条数据的所有属性
        SingleOutputStreamOperator<SensorReading> temperature2 = keyedStream.maxBy("temperature",false);

        temperature.print();

        env.execute();
    }
}
