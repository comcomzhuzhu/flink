package com.zx.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName Window_AggF_WindowF
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Window_AggF_WindowF {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8777);
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = flatMapDS.keyBy(data -> data.f0);
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> aggDS = window.aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(Tuple2<String, Long> value, Long accumulator) {
                return accumulator + 1;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            //              只有会话窗口才有
            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
//             agg 的输出是 windowFC的输入   已经被处理过了
        }, new WindowFunction<Long, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                Long count = input.iterator().next();
                out.collect(Tuple3.of(window.getStart(), s, count));
            }
        });

        aggDS.print("aggDS");

    }
}
