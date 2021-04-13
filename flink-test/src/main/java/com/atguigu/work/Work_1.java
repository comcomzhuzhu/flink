package com.atguigu.work;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


public class Work_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textFileDS = env.readTextFile("flink-test/input/UserBehavior.csv");

        env.setParallelism(1);

        textFileDS.process(new ProcessFunction<String, Long>() {
            long count = 0L;
            @Override
            public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {
                String[] strings = value.split(",");
                if ("pv".equals(strings[3])) {
                    count++;
                }
                out.collect(count);
            }
        }).print("11");

//        proDS.process(new ProcessFunction<Long, Long>() {
//            long pv=0L;
//            @Override
//            public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
//                pv+=value;
//                out.collect(pv);
//            }
//        }).setParallelism(1).print("pv");


        env.execute();
    }
}
