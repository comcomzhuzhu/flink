package com.atguigu.statetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName State_StateBackend
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/15 15:26
 * @Version 1.0
 */
public class State_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        状态后端配置

        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/checkpoints/fs"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/checkpoints/rocksdb"));

//         默认500ms做一次 ck 可以手动传参
//         还可以设置 状态级别
//        this method selects
//	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
        env.enableCheckpointing(500);

        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        做ck的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(2000);

//        当前同时进行的最大ck数    多个task 可能 同时出发 要做ck
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

//        前一次ck 保存结束 到下一次 ck开始  的最小暂歇时间 会影响之前的 ck间隔的时间
//         这个参数 提供了 ck 的结束和下一次开始 之间的 间隔时间 当并发为1 的时候 会影响之前的时间间隔配置
//        如果ck 之间的间隔小于100  会强行提供100ms的间隔时间 如果时间间隔大于100ms 就按照之前的配置触发
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);

//        env.getCheckpointConfig().setPreferCheckpointForRecovery();
//        默认0  ck挂了 代表任务挂了 重启  default value is 0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

//       重启策略     固定延迟重启                参数       次数 间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,20000L));

//        失败率重启   参数   只允许一段时间内失败次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));


        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> dataDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });


        env.execute();
    }
}
