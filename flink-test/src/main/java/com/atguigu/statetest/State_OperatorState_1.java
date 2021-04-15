package com.atguigu.statetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName State_OperatorState_1
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/14 21:03
 * @Version 1.0
 */
public class State_OperatorState_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> dataDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

//        定义一个有状态的map操作 统计当前分区数据个数
        SingleOutputStreamOperator<Long> re0DS = dataDS.map(new MyCountMapper());

        re0DS.print();
        env.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading, Long>, ListCheckpointed<Long> {

        //        定义在taskManager的本地变量 算子状态 所有key共享
//        如果要做键控状态 要上下文 获取key  进行区分
        Long count = 0L;

        @Override
        public Long map(SensorReading value) {
            count++;
            return count;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Long> state) {
            for (Long aLong : state) {
                count += aLong;
            }
        }
    }

    public static class MyMapper2 implements MapFunction<SensorReading, Long>, CheckpointedFunction {

        ListState<Long> state;
        long count = 0L;

        @Override
        public Long map(SensorReading value) {
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("state", Long.class));

            state.get().forEach(c -> count += c);
        }
    }
}
