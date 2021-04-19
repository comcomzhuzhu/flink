package com.atguigu.statetest;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ValueState_Case {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        OutputTag<String> outputTag = new OutputTag<String>("warning") {
        };

//        keyBy id
        SingleOutputStreamOperator<WaterSensor> reDS = caseClassDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private ValueState<Double> valueState;

                    @Override
                    public void open(Configuration parameters) {
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("value", Double.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        Double value1 = valueState.value();
                        if (value1 != null && Math.abs(value1 - value.getVc()) > 10) {
                            ctx.output(outputTag, value.getId() + "水位跳变");
                        }

//                      将当前水位更新到状态
                        valueState.update(value.getVc());
                        out.collect(value);
                    }
                });


//        侧输出流获取

        reDS.print("re");
        reDS.getSideOutput(outputTag).print("side");


        env.execute();
    }
}
