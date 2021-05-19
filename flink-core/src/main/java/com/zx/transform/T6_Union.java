package com.zx.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName T6_Union
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class T6_Union {
//     TODO 可以合并多条流  数据类型必须一样
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 =env.fromElements(11,12,32,37,44,55,11);
        DataStreamSource<Integer> stream3 =env.fromElements(100,100,200,300);


        DataStream<Integer> unionDS = stream1.union(stream2, stream3);


        env.execute();
    }
}
