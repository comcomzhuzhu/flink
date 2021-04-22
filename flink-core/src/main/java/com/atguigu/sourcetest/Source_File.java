package com.atguigu.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Source_File
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 10:10
 * @Version 1.0
 */
public class Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.readTextFile("flink-test/input/sensor.txt");

//         输出不按顺序 如果要按顺序 设置并行度1
        dataStream.print("dataStream");

        env.execute();


    }
}
