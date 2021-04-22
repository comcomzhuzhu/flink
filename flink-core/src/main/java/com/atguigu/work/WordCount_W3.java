package com.atguigu.work;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WordCount_W3 {
    private static final Logger logger = LoggerFactory.getLogger(WordCount_W3.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
//        data from socket ass water mark near source  for the last data only need put one time
        SingleOutputStreamOperator<String> withWMDS = socketTextStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
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


////          每个key 的wordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCDS = caseClassDS.keyBy(WaterSensor::getId)
                .flatMap(new MyRichFlatMapTopNFuncc());

////        collect all data in one partition to compute  top3 of all keys
        DataStream<Tuple2<String, Integer>> globalDS = wordCDS.global();
        globalDS.print("global");

//        window all   sliding event time  of  window size 30S   sliding every 10s
        globalDS
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
                    MapState<String, Integer> mapState;
                    HashMap<String, Integer> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) {
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("map",
                                        String.class, Integer.class)
                        );
                    }

                    private boolean isLate;
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) {


                        System.out.println(context.window().getStart() + " " + context.window().getEnd());

                        System.out.println(context.window().maxTimestamp());

                        if (isLate) {
                            ArrayList<Tuple2<String, Integer>> eList = Lists.newArrayList(elements.iterator());
                            Tuple2<String, Integer> tuple2 = eList.get(eList.size() - 1);
                            if (hashMap.containsKey(tuple2.f0)) {
                                hashMap.put(tuple2.f0, hashMap.get(tuple2.f0) + 1);
                            } else {
                                hashMap.put(tuple2.f0, 1);
                            }
                        } else {
                            for (Tuple2<String, Integer> element : elements) {
                                if (hashMap.containsKey(element.f0)) {
                                    hashMap.put(element.f0, element.f1 > hashMap.get(element.f0) ? element.f1 : hashMap.get(element.f0));
                                } else {
                                    hashMap.put(element.f0, element.f1);
                                }
                            }
                        }
                        ArrayList<Map.Entry<String, Integer>> entries = Lists.newArrayList(hashMap.entrySet().iterator());

                        entries.sort(((o1, o2) -> -o1.getValue().compareTo(o2.getValue())));

                        for (int i = 0; i < Math.min(3, hashMap.size()); i++) {
                            Map.Entry<String, Integer> entry = entries.get(i);
                            out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
                        }

                        isLate = true;
                    }
                })
                .print("word").setParallelism(1);


//        globalDS
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
////                 every 10S output a result  and in 2s every data come will update the result
////                 after 2s  the window really close
//                .allowedLateness(Time.seconds(2))
//                .aggregate(new MyAggTopNFunc()).print("agg").setParallelism(1);

        env.execute();
    }

    public static class MyAggTopNFunc implements AggregateFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, Tuple2<String, Integer>> {


        @Override
        public List<Tuple2<String, Integer>> createAccumulator() {
            return new ArrayList<>(3);
        }

        @Override
        public List<Tuple2<String, Integer>> add(Tuple2<String, Integer> value, List<Tuple2<String, Integer>> accumulator) {
            if (accumulator.size() < 3) {
                accumulator.add(value);
            } else {
                accumulator.sort(((o1, o2) -> -o1.f1.compareTo(o2.f1)));
                if (value.f1 > accumulator.get(2).f1) {
                    for (int i = 0; i < 3; i++) {
                        if (value.f1 > accumulator.get(i).f1) {
                            accumulator.add(i, value);
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public Tuple2<String, Integer> getResult(List<Tuple2<String, Integer>> accumulator) {
            return null;
        }

        @Override
        public List<Tuple2<String, Integer>> merge(List<Tuple2<String, Integer>> a, List<Tuple2<String, Integer>> b) {
            return null;
        }
    }


    public static class MyRichFlatMapTopNFuncc extends RichFlatMapFunction<WaterSensor, Tuple2<String, Integer>> {

        private ValueState<Tuple2<String, Integer>> valueState;

        @Override
        public void open(Configuration parameters) {
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("value",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                            }))
            );
        }

        @Override
        public void flatMap(WaterSensor value, Collector<Tuple2<String, Integer>> out) throws Exception {

            if (valueState.value() == null) {
                valueState.update(Tuple2.of(value.getId(), 0));
            }
            valueState.update(Tuple2.of(value.getId(), valueState.value().f1 + 1));
            out.collect(valueState.value());
        }
    }
}