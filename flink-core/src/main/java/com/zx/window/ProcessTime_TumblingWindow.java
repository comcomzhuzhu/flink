package com.zx.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessTime_TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8899);
        KeyedStream<Tuple2<String, Long>, String> keyedStream = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        }).keyBy(t -> t.f0);
//                offset  和 左闭右开是因为-1 如果开窗口是days(1)  要偏移量时区-8
        /**
         * Gets the largest timestamp that still belongs to this window.
         *
         * <p>This timestamp is identical to {@code getEnd() - 1}.
         *
         * @return The largest timestamp that still belongs to this window.
         *
         * @see #getEnd()
//         */
//        @Override
//        public long maxTimestamp() {
//            return end - 1;
//        }
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream =
                keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        windowedStream.allowedLateness(Time.milliseconds(100));

        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)));


        windowedStream.sum(1)
                .print();

        env.execute();
    }
}
