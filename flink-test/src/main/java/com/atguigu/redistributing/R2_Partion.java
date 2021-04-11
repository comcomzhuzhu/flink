package com.atguigu.redistributing;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName R2_Partion
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 22:11
 * @Version 1.0
 */
public class R2_Partion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        dss.print("input");

//        shuffle  随机打乱
        dss.shuffle().print("shuffle");

        SingleOutputStreamOperator<SensorReading> dataDS = dss.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));

            }
        });
//           keyBy  按照hashCode 重分区
        dataDS.keyBy("id");

//        global
        dataDS.global().print("global");

        env.execute();
    }
}
