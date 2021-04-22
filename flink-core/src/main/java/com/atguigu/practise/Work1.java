package com.atguigu.practise;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Work1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop", 7111);

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2DS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = tuple2DS.keyBy(data -> data.f0);
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple3<Long,String,Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                        ArrayList<Tuple2<String, Long>> list = Lists.newArrayList(elements.iterator());
                        long size = list.size();
                        long start = context.window().getStart();
                        out.collect(Tuple3.of(start,s,size));
                    }
                }).print("window process window Func");


        env.execute();
    }
}
