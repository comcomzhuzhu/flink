package com.zx.statetest;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class State_Test2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> dataDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

//        统计当前 sensor的数据个数
        SingleOutputStreamOperator<Long> re0DS = dataDS.keyBy(SensorReading::getId)
                .map(new MyKeyCountMapper());

        re0DS.print();
        env.execute();
    }

    private static class MyKeyCountMapper extends RichMapFunction<SensorReading, Long> {
        private ValueState<Long> keyCountState;
//        其他类型状态的声明
        private ListState<String> myListState;
//        map 类型
        private MapState<String,Double> myMapState;

        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) {
            keyCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("key-count", Long.class));
            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("my-list", String.class)
            );
            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("my-map",
                            String.class, Double.class)
            );
            myReducingState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<>(
                            "reducing"
                            , new ReduceFunction<SensorReading>() {
                        @Override
                        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                            return new SensorReading(value2.getId(), value2.getTimeStamp(),
                                    value1.getTemperature() + value2.getTemperature());
                        }
                    }, SensorReading.class)
            );
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public Long map(SensorReading value) throws Exception {
            myReducingState.clear(); //所有状态都有清空方法
//            reducing state
            myReducingState.add(new SensorReading());  //add 调用自己定义的聚合方法

            myMapState.get("1");
            myMapState.put("2", 2.4);
            myMapState.remove("2");

            Long aLong = keyCountState.value();
            if (null == aLong)
                aLong = 0L;
            aLong++;
            keyCountState.update(aLong);

//            其他状态API调用
            Iterable<String> strings = myListState.get();
            for (String string : strings) {
                System.out.println(string);
            }

            //myListState.update(); //覆盖之前的list
            myListState.add("hello"); //追加一个元素
//            myListState.addAll();  加一个列表

            return aLong;
        }
    }
}
