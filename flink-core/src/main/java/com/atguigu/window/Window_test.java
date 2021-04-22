package com.atguigu.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class Window_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        TODO 有界流 无法等15S再执行 直接结束了 需要换成无界流
//        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);


        SingleOutputStreamOperator<SensorReading> caseClassDS = dss.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] strings = value.split(",");
                return new SensorReading(strings[0], Long.valueOf(strings[1]),
                        Double.valueOf(strings[2]));
            }
        });
        caseClassDS.print("case");

        caseClassDS.map(t -> t)
                .keyBy("id")
//                .timeWindow(Time.seconds(15))  TODO  过期的方法 不要用了
                .minBy("id");

        SingleOutputStreamOperator<Long> re0DS = caseClassDS
                .keyBy("id")
//                 TODO keyBy 分组之后 每个组都会开窗
//                .timeWindow(Time.seconds(15))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .reduce(new ReduceFunction<SensorReading>() {
//                    @Override
//                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                        return null;
//                    }
//                })

                .aggregate(new AggregateFunction<SensorReading, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(SensorReading value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                });
        re0DS.print();

        env.execute();

    }
}
