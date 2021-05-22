package com.zx.examples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Project_Order
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Project_Order {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> orderDS = env.readTextFile("");
        DataStreamSource<String> txDS = env.readTextFile("");





        env.execute();
    }
}
