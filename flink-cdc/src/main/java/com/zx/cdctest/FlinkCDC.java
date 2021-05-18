package com.zx.cdctest;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        开启间隔10min 就做一次ck
        env.enableCheckpointing(60*1000L*10);
//        开启任务关闭时 保留最后一次ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//       设置状态后端位置
        env.setStateBackend(new FsStateBackend("hadoop102:8020/flink-ck"));

        DebeziumSourceFunction<String> source = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.z_user_info")
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> mysqlDS = env.addSource(source);


        mysqlDS.print();


        env.execute();
    }
}
