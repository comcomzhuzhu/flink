package com.zx.work;

import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ClassName WordCount_W2
 * @Description TODO
 * @Author Xing
 * @Date 19 16:24
 * @Version 1.0
 */
public class WordCount_W2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

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

        SingleOutputStreamOperator<Tuple2<String, Integer>> wcDS = caseClassDS.keyBy(WaterSensor::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .aggregate(new AggregateFunction<WaterSensor, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of("", 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(WaterSensor value, Tuple2<String, Integer> accumulator) {
                        return Tuple2.of(value.getId(), accumulator.f1 + 1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                });

        DataStream<Tuple2<String, Integer>> broadcast = wcDS.broadcast();
        broadcast.process(new MyProWork2()).print("work").setParallelism(1);

        env.execute();
    }


    public static class MyProWork2 extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> implements CheckpointedFunction {

        ListState<Tuple2<String, Integer>> listState;
        HashMap<String, Integer> map = new HashMap<>();

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Tuple2<String, Integer>>("list",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                                @Override
                                public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
                                    return super.getTypeInfo();
                                }
                            }))
            );
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            if (map.containsKey(value.f0)) {
                Integer integer = map.get(value.f0);
                map.put(value.f0, value.f1 > integer ? value.f1 : integer);
            } else {
                map.put(value.f0, value.f1);
            }

            ArrayList<Integer> arrayList = Lists.newArrayList(map.values().iterator());

//            Iterator<Tuple2<String, Integer>> iterator = listState.get().iterator();
//
//            ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(iterator);

            arrayList.sort(((o1, o2) -> -o1.compareTo(o2)));


            for (int i = 0; i < Math.min(3, arrayList.size()); i++) {
//                out.collect(tuple2);
            }
        }
    }

}