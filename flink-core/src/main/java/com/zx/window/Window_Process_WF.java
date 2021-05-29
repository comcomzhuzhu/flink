package com.zx.window;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Window_Process_WF {
    public static void main(String[] args) throws Exception {
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

        SingleOutputStreamOperator<Tuple3<Long, String, Long>> processDS = window.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple3<Long, String, Long>> out) {
                ArrayList<Tuple2<String, Long>> list = Lists.newArrayList(elements.iterator());

                out.collect(Tuple3.of(context.window().getStart(), s, (long) list.size()));
            }
        });

        processDS.print("process window");
        env.execute();
    }
}
