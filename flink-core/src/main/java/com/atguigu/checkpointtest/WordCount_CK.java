package com.atguigu.checkpointtest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @ClassName WordCount_CK
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/20 11:12
 * @Version 1.0
 */
public class WordCount_CK {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-ck"));

//        开启ck
        env.enableCheckpointing(2000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

//        两个ck 之间必须间隔的时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(200);

//        如果 遇到一个错误数据 比如从kafka读数据 会一直重复处理错误数据 一直在重启
//        重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(5), Time.seconds(10)));

//        读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "aaa");
        DataStreamSource<String> first = env.addSource(new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties));

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);

        first.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                System.out.println(1 / 0);
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(value -> value.f0).sum(1).print();


        env.execute();
    }
}
