package com.zx.statetest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class MapState_Case {
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


        caseClassDS.keyBy(WaterSensor::getId)
//                实现每个传感器传输的水位线值进行去重
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private MapState<Double, String> mapState;

                    @Override
                    public void open(Configuration parameters) {
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>(
                                        "map"
                                        , Double.class, String.class)
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

//      TODO        判断当前水位线是否在状态已存在
                        if (!mapState.contains(value.getVc())) {
                            mapState.put(value.getVc(), null);
                            out.collect(value);
                        }
                    }
                })
                .print("map");

        env.execute();
    }
}
