package com.zx.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**TODO
 * 	在keyed streams上使用窗口, 窗口计算被并行的运用在多个task上,
 * 	可以认为每个分组都有自己单独窗口. 正如前面的代码所示.
 * 	在non-keyed stream上使用窗口, 流的并行度只能是1,
 * 	所有的窗口逻辑只能在一个单独的task上执行.
 * 	需要注意的是: 非key分区的流, 即使把并行度设置为大于1 的数, 窗口也只能在某个分区上使用
 */
public class WindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8899);
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
