package com.atguigu.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName CountWindowTest
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/14 16:25
 * @Version 1.0
 */
public class CountWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8899);
        KeyedStream<Tuple2<String, Long>, String> keyedStream = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        }).keyBy(t -> t.f0);

//        输出 xxx 5  keyBy了   必须要 五个hello才有输出
        keyedStream.countWindow(5)
                .sum(1)
                .print("tumbling");


        keyedStream.countWindow(5,2)
                .sum(1)
                .print("sliding");


        env.execute();
    }
}
