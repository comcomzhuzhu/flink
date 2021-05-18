package com.zx.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @ClassName Window_ReduceWindowFC
 * @Description TODO
 * @Author Xing
 * @Date 16 9:51
 * @Version 1.0
 */
public class Window_ReduceWindowFC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8777);
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
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        window.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                return null;
            }
        }, new WindowFunction<Tuple2<String, Long>, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple3<Long, String, Long>> out) {

            }
        });


        env.execute();
    }
}
