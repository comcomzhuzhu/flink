package com.zx.statetest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.util.Collection;
import java.util.HashMap;


public class WordCount_Work2 {
    private static final Logger logger = LoggerFactory.getLogger(WordCount_Work2.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);

//        data from socket ass water mark near source  for the last data only need put one time
        SingleOutputStreamOperator<String> withWMDS = socketTextStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.valueOf(split[1]) * 1000000L;
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


//          keyBy and out the top3 of every key in richFlatMapFunc
        SingleOutputStreamOperator<Tuple3<Double, String, Integer>> flatMapDS = caseClassDS.keyBy(WaterSensor::getId)
                .flatMap(new MyRichFlatMapTopNFunc(3));

//        collect all data in one partition to compute  top3 of all keys
        DataStream<Tuple3<Double, String, Integer>> globalDS = flatMapDS.global();
        globalDS.print();

//        window all   sliding event time  of  window size 30S   sliding every 10s
        globalDS
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
//                 every 10S output a result  and in 2s every data come will update the result
//                 after 2s  the window really close
                .allowedLateness(Time.seconds(2))
//                     process sort all elements
                .process(new ProcessAllWindowFunction<Tuple3<Double, String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public void open(Configuration parameters) {
                        listState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("list",
                                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                                        })
                                ));

                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("map",
                                        String.class, Integer.class)
                        );
                    }

                    ListState<Tuple2<String, Integer>> listState;
                    MapState<String, Integer> mapState;
                    HashMap<Double, Tuple2<String, Integer>> hashMap = new HashMap<>();
                    @Override
//              TODO       调用了三次  一个很大的时间戳 关闭了三个窗口
                    public void process(Context context, Iterable<Tuple3<Double, String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

//                         add element to a map if key exist get it and compare number
//                         use the bigger
                        for (Tuple3<Double, String, Integer> element : elements) {
                            if (hashMap.containsKey(element.f0)) {
                                Tuple2<String, Integer> tuple2 = hashMap.get(element.f0);
                                if (tuple2.f0.equals(element.f1)) {
                                    hashMap.put(element.f0, Tuple2.of(element.f1, tuple2.f1 > element.f2 ? tuple2.f1 : element.f2));
                                } else {
                                    hashMap.put(element.f0, Tuple2.of(element.f1, element.f2));
                                }
                            } else {
                                hashMap.put(element.f0, Tuple2.of(element.f1, element.f2));
                            }
                        }

                        ArrayList<Double> arrayList = Lists.newArrayList(hashMap.keySet().iterator());
//                        sort the map keys
                        arrayList.sort(((o1, o2) -> -o1.compareTo(o2)));

                        if (arrayList.size() >= 3) {
                            for (int i = 0; i < 3; i++) {
                                Tuple2<String, Integer> entry = hashMap.get(arrayList.get(i));
                                mapState.put(entry.f0, entry.f1);
                            }
                        } else {
                            Collection<Tuple2<String, Integer>> values = hashMap.values();
                            for (Tuple2<String, Integer> value : values) {
                                mapState.put(value.f0, value.f1);
                            }
                        }

                        mapState.entries().forEach(Entry ->
                                out.collect(Tuple2.of(Entry.getKey(), Entry.getValue())));

                    }
                })
                .print("work2").setParallelism(1);

        env.execute();
    }

    public static class MyRichFlatMapTopNFunc extends RichFlatMapFunction<WaterSensor, Tuple3<Double, String, Integer>> {
        private int topSize;
        private MapState<Double, Tuple2<String, Integer>> mapState;
        volatile ArrayList<Double> top3 = new ArrayList<>();

        @Override
        public void open(Configuration parameters) {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map",
                    TypeInformation.of(Double.class),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                    })
            ));
        }

        public MyRichFlatMapTopNFunc(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void flatMap(WaterSensor value, Collector<Tuple3<Double, String, Integer>> out) throws Exception {

            if (mapState.contains(value.getVc())) {
                Tuple2<String, Integer> tuple2 = mapState.get(value.getVc());
                mapState.put(value.getVc(), Tuple2.of(value.getId(), tuple2.f1 + 1));
            } else {
                mapState.put(value.getVc(), Tuple2.of(value.getId(), 1));
            }

            ArrayList<Double> keys = Lists.newArrayList(mapState.keys().iterator());

            keys.sort(((o1, o2) -> -o1.compareTo(o2)));

            System.out.println(keys);

            if (keys.size() >= 3) {
                for (int i = 0; i < 3; i++) {
                    top3.add(keys.get(i));
                }
            }

            if (top3.size() >= 3) {
                for (Double aDouble : mapState.keys()) {
                    if (!top3.contains(aDouble)) {
                        mapState.remove(aDouble);
                    }
                }
            }

            System.out.println(mapState.keys());

            for (int i = 0; i < Math.min(keys.size(), 3); i++) {
                Double aDouble = keys.get(i);
                Tuple2<String, Integer> tuple2 = mapState.get(aDouble);
                out.collect(Tuple3.of(aDouble, tuple2.f0, tuple2.f1));
            }

        }
    }
}