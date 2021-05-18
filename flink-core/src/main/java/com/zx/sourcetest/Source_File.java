package com.zx.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("flink-test/input/sensor.txt");

//         输出不按顺序 如果要按顺序 设置并行度1
        dataStream.print("dataStream");

        env.execute();


    }
}
