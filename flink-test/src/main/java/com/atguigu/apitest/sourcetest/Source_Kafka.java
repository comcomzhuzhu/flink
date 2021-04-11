package com.atguigu.apitest.sourcetest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName Source_Kafka
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 10:15
 * @Version 1.0
 */
public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        pro.setProperty("group.id", "test1");
        pro.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaDSS = env.addSource(new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), pro));


        kafkaDSS.print();
        env.execute("kafkaDSS");
    }
}
