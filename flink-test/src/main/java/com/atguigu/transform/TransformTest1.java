package com.atguigu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName TransformTest1
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 16:32
 * @Version 1.0
 */
public class TransformTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile("flink-test/input/test1.txt");

        SingleOutputStreamOperator<String> wordDS = dss.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(",");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });


        wordDS.print();
        SingleOutputStreamOperator<Integer> length = wordDS.map(String::length);

        length.print();
        SingleOutputStreamOperator<String> string1 = length.map(new MapFunction<Integer, String>() {
            @Override
            public String map(Integer value) {
                return value.toString();
            }
        });

        string1.print();

        SingleOutputStreamOperator<String> filterDS = string1.filter((FilterFunction<String>) value -> Integer.parseInt(value) % 2 == 0);

        filterDS.print("xxxxx");




        env.execute();
    }
}
