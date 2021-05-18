package com.zx.sourcetest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        pro.setProperty("group.id", "test1");
        pro.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> first = new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), pro);
        first.setStartFromGroupOffsets();
        DataStreamSource<String> kafkaDSS = env.addSource(first);


        kafkaDSS.print();
        env.execute("kafkaDSS");
    }
}
