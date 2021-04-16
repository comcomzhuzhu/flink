package com.atguigu.watermarktest;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @ClassName WindowET_SW
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/16 20:37
 * @Version 1.0
 */
public class WindowET_SW {
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
                Duration.ofMillis(2)
        ).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        KeyedStream<WaterSensor, String> keyedStream = andWatermarks.keyBy(WaterSensor::getId);

        keyedStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(5)))
                .sum("vc")
                .print("s");




        env.execute();
    }
}
