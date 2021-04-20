package com.atguigu.statetest;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.xml.crypto.dom.DOMCryptoContext;

/**
 * @ClassName AggState_Case
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/19 14:28
 * @Version 1.0
 */
public class AggState_Case {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        caseClassDS.keyBy(WaterSensor::getId)
//                每个传感器的平均水位
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Double>>() {

                    private AggregatingState<Double, Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) {
                        aggregatingState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Double, Tuple2<Double, Integer>, Double>(
                                        "agg",
                                        new AggregateFunction<Double, Tuple2<Double, Integer>, Double>() {
                                            @Override
                                            public Tuple2<Double, Integer> createAccumulator() {
                                                return Tuple2.of(0.0, 0);
                                            }

                                            @Override
                                            public Tuple2<Double, Integer> add(Double value, Tuple2<Double, Integer> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Double getResult(Tuple2<Double, Integer> accumulator) {
                                                return accumulator.f0 / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                            }
                                        }
                                        , TypeInformation.of(new TypeHint<Tuple2<Double, Integer>>() {})));
                    }
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                        aggregatingState.add(value.getVc());

                        out.collect(Tuple2.of(value.getId(),aggregatingState.get()));

                    }
                })
                .print("agg");


    }
}
