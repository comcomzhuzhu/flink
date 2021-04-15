package com.atguigu.statetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName State_KeyedApp
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/15 14:06
 * @Version 1.0
 */
public class State_KeyedApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> dataDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        
        env.execute();
    }
}
