package com.zx.sourcetest;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


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
