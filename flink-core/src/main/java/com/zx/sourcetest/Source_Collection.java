package com.zx.sourcetest;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;


public class Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dss = env.fromCollection(Arrays.asList(new SensorReading("sensor1", 1547718201L, 14.0),
                new SensorReading("sensor2", 1547712401L, 12.4),
                new SensorReading("sensor3", 1547718201L, 18.8))
        );

//     TODO   以第一个元素作为 type  后面的所有元素必须和这个type相同
        DataStreamSource<Object> oss = env.fromElements(new Object(), 213, 123, 213, "12", 4, 1, 1, 1);

        dss.print("sensor");
        oss.print("oss");
        env.execute("job");

    }
}
