package com.zx.transform;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName T3_Reduce
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class T3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> mapDS = dss.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = mapDS.keyBy("id");

//        reduce  最大的温度值 和最新的时间戳
        SingleOutputStreamOperator<SensorReading> re0OS = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
//             TODO      value1 是之前的结果  最新的数据是value2
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(), value2.getTimeStamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        SingleOutputStreamOperator<SensorReading> re1OS = keyedStream.reduce((curState, newData) ->
                new SensorReading(curState.getId(),
                        newData.getTimeStamp(),
                        Math.max(curState.getTemperature(), newData.getTemperature())));


        re0OS.print("aa");
        re1OS.print("bb");

        env.execute();
    }
}
