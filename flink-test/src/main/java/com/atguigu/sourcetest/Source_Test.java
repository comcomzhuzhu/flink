package com.atguigu.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName Source_Test
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 12:26
 * @Version 1.0
 */
public class Source_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<String> source = new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                System.out.println("run:" + this.hashCode());
            }

            @Override
            public void cancel() {
            }
        };
        DataStreamSource<String> dss = env.addSource(source);
        System.out.println("main:"+source.hashCode());
        dss.print();
        env.execute();

    }
}
