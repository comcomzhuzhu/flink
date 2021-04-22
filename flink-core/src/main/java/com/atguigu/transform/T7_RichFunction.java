package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

/**
 * @ClassName T7_RichFunction
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 21:46
 * @Version 1.0
 */
public class T7_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
//       TODO  不设置并行度的话 本地默认最大核心数
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> dataDS = dss.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
            }
        });

        dataDS.print("dataDS");
        DataStream<Tuple2<String, Integer>> result = dataDS.map(new MyMapper());

        result.print();
        env.execute();
    }

//    RichMapFunction 继承了抽象richFunction 实现了MapFunction
//    rich function  用继承
//    平常写的普通版本mapFunction 直接实现mapFunction
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        //    TODO 初始化工作 一般定义状态    或者建立数据库连接
        @Override
        public void open(Configuration parameters) {
//         TODO   open close 输出的次数是分区次数
//            每个分区都有 这个类的实例 每个分区输出一次
            System.out.println("open");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading value) {
//            getRuntimeContext();
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void close() {
            System.out.println("close");
        }
    }


}
