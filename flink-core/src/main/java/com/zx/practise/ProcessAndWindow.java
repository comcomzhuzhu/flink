package com.zx.practise;

import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;

public class ProcessAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> watermarksDS = caseClassDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        })
        );

        KeyedStream<WaterSensor, String> keyedStream = watermarksDS.keyBy(WaterSensor::getId);
        SingleOutputStreamOperator<Tuple2<String, Long>> re0DS = keyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("late") {
                })
                .process(new ProcessWindowFunction<WaterSensor, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Tuple2<String, Long>> out) {
                        ArrayList<WaterSensor> list = Lists.newArrayList(elements.iterator());
                        long size = list.size();
                        out.collect(Tuple2.of(s, size));
                    }
                });
        re0DS.print("result");
        re0DS.getSideOutput(new OutputTag<WaterSensor>("late") {
        }).print("side");
//      7s ??????   ????????? -25 5 ?????????
//      7-9?????? ???????????????-25???5??????????????? ?????????????????????????????????????????????
//       ??????????????????????????????????????????
//         ??????????????????


//        ?????????????????????????????? ??????????????????????????????????????????
//        ????????????????????????????????????????????????   ????????????????????????
        env.execute();
    }
}
