package com.atguigu.statebackend;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName StateBackend_Ck
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/20 10:17
 * @Version 1.0
 */
public class StateBackend_Ck {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/"));

        env.setStateBackend(new RocksDBStateBackend(""));

        env.execute();
    }
}
