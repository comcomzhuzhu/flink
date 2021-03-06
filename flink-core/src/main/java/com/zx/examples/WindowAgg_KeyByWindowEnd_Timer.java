package com.zx.examples;

import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

public class WindowAgg_KeyByWindowEnd_Timer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);
//        ass water mark near source  for the last data only need put one time
        SingleOutputStreamOperator<String> withWMDS = socketTextStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] split = element.split(",");
                                return Long.valueOf(split[1]) * 1000L;
                            }
                        }));

//        map to POJO
        SingleOutputStreamOperator<WaterSensor> caseClassDS = withWMDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        int tonN = 3;
        int allowedLate = 2;
//       keyBy  window  agg    and  add windowEnd to data
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> withTsDS = caseClassDS.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .allowedLateness(Time.seconds(allowedLate))
                .aggregate(new AggregateFunction<WaterSensor, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                }, new WindowFunction<Integer, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple3<String, Long, Integer>> out) {
                        System.out.println(window.getStart());
                        System.out.println(window.getEnd());
                        out.collect(Tuple3.of(key, window.getEnd(), input.iterator().next()));
                    }
                });

//        keyBy  WindowEND
        withTsDS.keyBy(tuple -> tuple.f1)
                .process(new MyTopNKeyedProFunction(tonN, allowedLate))
                .print("result").setParallelism(1);

        env.execute();
    }

    private static class MyTopNKeyedProFunction extends KeyedProcessFunction<Long, Tuple3<String, Long, Integer>, Tuple2<String, Integer>> {
        private int topN;
        private int allowedLate;
        //        ??????????????? ????????????
        private MapState<String, Integer> mapState;
        private boolean isLate = false;
        private Long closeTime = null;

        @Override
        public void open(Configuration parameters) {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map", String.class, Integer.class));
        }

        MyTopNKeyedProFunction(int topN, int allowedlate) {
            this.topN = topN;
            this.allowedLate = allowedlate;
        }

        @Override
        public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//            ??????????????????????????? ??????????????????????????????????????????
//            ?????????????????????End keyBy???
//            ???????????? ??????????????????????????????
            if (isLate) {
                mapState.put(value.f0, value.f2);
                sortAndOut(out);
            } else {
                mapState.put(value.f0, value.f2);
//             ????????????????????????????????????
                ctx.timerService().registerEventTimeTimer(value.f1 - 1);
//             ??????????????????????????????
                closeTime = value.f1 + allowedLate * 1000L - 1L;
                ctx.timerService().registerEventTimeTimer(closeTime);
            }
        }

        private void sortAndOut(Collector<Tuple2<String, Integer>> out) throws Exception {
            ArrayList<Map.Entry<String, Integer>> entries = Lists.newArrayList(mapState.entries().iterator());
//            ?????? ???????????? ??????????????????
            entries.sort(((o1, o2) -> -o1.getValue().compareTo(o2.getValue())));
//            ???????????????
            for (int i = 0; i < Math.min(topN, entries.size()); i++) {
                out.collect(Tuple2.of(entries.get(i).getKey(), entries.get(i).getValue()));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (timestamp == closeTime) {
//                ????????????
                mapState.clear();
            } else {
                sortAndOut(out);
                isLate = true;
            }
        }
    }
}