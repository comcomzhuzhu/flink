package com.zx.statetest;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName State_KeyedState_Api
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class State_KeyedState_Api {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("", 888);


        socketTextStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split(",")[0];
            }
        })
                .process(new MyKeyedProFunc());


        env.execute();
    }

    public static class MyKeyedProFunc extends KeyedProcessFunction<String, String, String> {
        //        定义状态
        private ValueState<String> valueState;
        private ListState<String> listState;
        private MapState<String, String> mapState;
        private ReducingState<String> reducingState;
        private AggregatingState<String, String> aggregatingState;

        //        初始化状态
        @Override
        public void open(Configuration parameters) {
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<String>("value", String.class)
            );

            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("list", String.class));

            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, String>("map", String.class, String.class));

            reducingState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<String>("reduce", new ReduceFunction<String>() {
                        @Override
                        public String reduce(String value1, String value2) throws Exception {
                            return null;
                        }
                    }, String.class)
            );
            aggregatingState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<String, Object, String>("agg",
                            new AggregateFunction<String, Object, String>() {
                                @Override
                                public Object createAccumulator() {
                                    return null;
                                }

                                @Override
                                public Object add(String value, Object accumulator) {
                                    return null;
                                }

                                @Override
                                public String getResult(Object accumulator) {
                                    return null;
                                }

                                @Override
                                public Object merge(Object a, Object b) {
                                    return null;
                                }
                            }, Object.class)
//                           这个类型是缓冲区的类型
            );

        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
//            使用状态
            String value1 = valueState.value();//读
            valueState.update(value); //写 改
            valueState.clear();  //删

            listState.add(value);
            listState.addAll(new ArrayList<>()); //append

            Iterable<String> strings = listState.get();
            listState.update(new ArrayList<>()); //override
            listState.clear();

            mapState.contains("");
            mapState.put("", "");
            mapState.putAll(new HashMap<>());

            mapState.get("");
            Iterable<Map.Entry<String, String>> entries = mapState.entries();
            mapState.keys();
            mapState.values();
            mapState.remove("");
            mapState.clear();

//            reduce
            String s = reducingState.get();
            reducingState.add("");//会调用reduceFunc
            reducingState.clear();


            aggregatingState.add("");
            aggregatingState.get();

        }
    }
}
