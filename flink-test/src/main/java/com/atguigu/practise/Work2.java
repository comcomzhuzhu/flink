package com.atguigu.practise;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName Work2
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/16 10:58
 * @Version 1.0
 */
public class Work2 {
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

        tuple2DS.keyBy(data->data.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.minutes(2)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        return accumulator+1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a+b;
                    }
                }, new WindowFunction<Long, Tuple3<Long, String, Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                        Long next = input.iterator().next();
                        out.collect(Tuple3.of(window.getStart(), s,next));
                    }
                });

        env.execute();
    }
}