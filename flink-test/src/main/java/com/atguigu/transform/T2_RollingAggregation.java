package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName T2_RollingAggregation
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 16:52
 * @Version 1.0
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
//      只改变了一个字段 其他的字段还是来自第一条数据 滚动聚合
        maxT.print();

//        所有字段都变成了 最大值对应的 属性了
        SingleOutputStreamOperator<SensorReading> temperature = keyedStream.maxBy("temperature");

        temperature.print();


        env.execute();
    }
}
