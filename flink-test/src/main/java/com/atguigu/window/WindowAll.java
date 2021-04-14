package com.atguigu.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName WindowAll
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/14 16:47
 * @Version 1.0
 */
public class WindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8899);
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });


        streamOperator.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                sum 取了第一个元素
        .sum(1).print();

        env.execute();
    }
}
