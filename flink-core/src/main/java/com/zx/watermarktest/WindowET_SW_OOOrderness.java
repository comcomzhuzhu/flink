package com.zx.watermarktest;


import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName WindowET_SW_OOOrdernes
 * @Description TODO
 * @Author Xing
 * 16 15:19
 * @Version 1.0
 */
public class WindowET_SW_OOOrderness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 3333);

        SingleOutputStreamOperator<WaterSensor> dataDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> andWatermarks = dataDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                Duration.ofSeconds(2)
        ).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        KeyedStream<WaterSensor, String> keyedStream = andWatermarks.keyBy(WaterSensor::getId);


        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
                .allowedLateness(Time.seconds(2))

                .process(new ProcessWindowFunction<WaterSensor, Integer, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<Integer> out) throws Exception {
                        System.out.println(context.window().maxTimestamp());
                        System.out.println(context.window().getStart() + "-" + context.window().getEnd());
                        int size = Lists.newArrayList(elements.iterator()).size();
                        out.collect(size);
                    }
                }).print().setParallelism(1);


        env.execute();
    }

}
