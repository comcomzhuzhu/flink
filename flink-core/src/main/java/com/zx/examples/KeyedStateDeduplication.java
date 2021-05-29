package com.zx.examples;

import com.alibaba.fastjson.JSON;
import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName KeyedStateDeduplication
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class KeyedStateDeduplication {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://zx101:8020/flink-ck/deduplication", true);
        rocksDBStateBackend.setNumberOfTransferThreads(3);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);

        rocksDBStateBackend.isIncrementalCheckpointsEnabled();
        env.setStateBackend(rocksDBStateBackend);

        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));

        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(9));
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("", new SimpleStringSchema(), properties));
        dataStreamSource.map(line -> JSON.parseObject(line, WaterSensor.class))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private ValueState<Boolean> isExist;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("state", Boolean.class);
                        StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        stateDescriptor.enableTimeToLive(ttlConfig);
                        isExist = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<WaterSensor> collector) throws Exception {
                        if (null == isExist.value()) {
                            isExist.update(true);
                            collector.collect(waterSensor);
                        } else {

                        }
                    }
                });

    }
}
