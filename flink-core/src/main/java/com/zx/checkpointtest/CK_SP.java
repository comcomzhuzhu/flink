package com.zx.checkpointtest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class CK_SP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new FsStateBackend("hdfs://zx101:8020/flink-ck"));
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000L);

        env.getCheckpointConfig().getCheckpointTimeout();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(3), Time.seconds(5)));

//        env.getCheckpointConfig().enableExternalizedCheckpoints();

//        Properties properties = new Properties();
//
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zx101:9092,hadoop103:9092,hadoop104:9092");
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ccc");
//        DataStreamSource<String> dataDS = env.addSource(new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties));


        DataStreamSource<String> dataDS = env.socketTextStream("zx101", 8777);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordDS = dataDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        wordDS.keyBy(data -> data.f0)
                .sum(1)
                .print();


        env.execute();
    }
}
