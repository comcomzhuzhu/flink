package com.atguigu.statetest;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @ClassName ListState_Case
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/19 13:49
 * @Version 1.0
 */
public class ListState_Case {
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


        caseClassDS.keyBy(WaterSensor::getId)
                .flatMap(new MyRichFlatMapTopNFunc(3))
                .print("test");

        env.execute();
    }

    public static class MyRichFlatMapTopNFunc extends RichFlatMapFunction<WaterSensor,WaterSensor> {
        private Integer topSize;

        private ListState<WaterSensor> listState;

        @Override
        public void open(Configuration parameters) {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<>("list-state",
                    WaterSensor.class));
        }

        public MyRichFlatMapTopNFunc(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void flatMap(WaterSensor value, Collector<WaterSensor> out) throws Exception {
            listState.add(value);
//            取出所有数据
            Iterator<WaterSensor> iterator = listState.get().iterator();
            ArrayList<WaterSensor> waterSensors = Lists.newArrayList(iterator);

            waterSensors.sort(new Comparator<WaterSensor>() {
                @Override
                public int compare(WaterSensor o1, WaterSensor o2) {
                    return (int) (o2.getVc() - o1.getVc());
                }
            });







        }
    }
}