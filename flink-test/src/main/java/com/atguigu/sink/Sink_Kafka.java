package com.atguigu.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName Sink_Kafka
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/12 19:05
 * @Version 1.0
 */
public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDS = env.readTextFile("flink-test/input/sensor.txt");
        SingleOutputStreamOperator<String> caseClassDS = textDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])).toString();
            }
        });

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("hadoop102:9092", "first", new SimpleStringSchema());

        caseClassDS.addSink(kafkaProducer);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        caseClassDS.addSink(new FlinkKafkaProducer<String>("first", new SimpleStringSchema(), properties));

        env.execute();
    }
}
