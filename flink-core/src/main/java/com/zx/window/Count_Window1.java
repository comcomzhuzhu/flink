package com.zx.window;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Count_Window1
 * @Description TODO
 * @Author Xing
 * @Date 13 18:56
 * @Version 1.0
 */
public class Count_Window1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);

        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

//  TODO 开一个计数窗口 测试  统计十个数据的平均温度值

        caseDS.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) {
                return value.getId();
            }
//            每隔两个数 滑动一次    两个输出一个   输出频率由滑动步长决定
        }).countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return Tuple2.of(0.0, 0);
                    }

//              accumulator 就是之前的结果
                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        double f0 = accumulator.f0 + value.getTemperature();
                        int f1 = accumulator.f1 + 1;
                        return Tuple2.of(f0, f1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return Tuple2.of(a.f0+b.f0, a.f1+b.f1);
                    }
                }).print("avg temp");


        env.execute();
    }
}
