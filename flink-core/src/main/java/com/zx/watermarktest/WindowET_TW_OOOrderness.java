package com.zx.watermarktest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName WindowET_TW_OOOrderness
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class WindowET_TW_OOOrderness {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8877);

        SingleOutputStreamOperator<WaterSensor> dataDS = socketTextStream.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) {
                String[] strings = value.split(",");
                out.collect(new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2])));
            }
        });

        dataDS.print("data");
        SingleOutputStreamOperator<WaterSensor> waterMarkDS = dataDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));
        waterMarkDS.print("water");
        SingleOutputStreamOperator<WaterSensor> re0DS = waterMarkDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("late") {
                })
                .sum("vc");

        re0DS.print("result");

        re0DS.getSideOutput(new OutputTag<WaterSensor>("late") {
        }).print("side");

        env.execute();
    }
}
