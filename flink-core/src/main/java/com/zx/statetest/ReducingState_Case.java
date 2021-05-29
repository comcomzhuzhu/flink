package com.zx.statetest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ReducingState_Case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

//        累加水位线
        caseClassDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Double>>() {
                    //                   定义状态
                    private ReducingState<Double> reducingState;

                    @Override
                    public void open(Configuration parameters) {
                        reducingState = getRuntimeContext().getReducingState(
                                new ReducingStateDescriptor<>("reduce",
                                        (ReduceFunction<Double>) (value1, value2) -> value1 + value2, Double.class)
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                        reducingState.add(value.getVc());
                        out.collect(Tuple2.of(value.getId(), reducingState.get()));
                    }
                })
        .print("sum");


        env.execute();
    }
}
