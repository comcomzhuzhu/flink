package com.zx.join;

import com.zx.bean.Table1;
import com.zx.bean.Table2;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName Join
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Join {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Table2> table2 = env.socketTextStream("zx101", 1111)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Table2(split[0], split[1]);
                });
        SingleOutputStreamOperator<Table1> table1 = env.socketTextStream("zx101", 2222)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Table1(split[0], split[1]);
                });
//        注册表
        tableEnvironment.createTemporaryView("table1", table1);
        tableEnvironment.createTemporaryView("table2", table2);

//        默认join的状态一直存    可以设置清理参数
        System.out.println(tableEnvironment.getConfig().getIdleStateRetention());

        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

//        join  join  leftJoin  fullJoin
//        普通的join 两条流的状态策略都是 OnCreateAndWrite 只在创建和写的时候
//        也就是自己流的数据来的时候更新
        tableEnvironment.sqlQuery("select table1.id," +
                "table1.name,table2.sex " +
                "from table1 join table2 on table1.id=table2.id");


//        leftJoin  先出来一个null   连上之后 出现一个-D 删除  然后I插入一条数据


//        leftJoin 以左边为主 如果右边的流在10S内一直来数据
//        左边的数据状态一直更新存10S
//        左边的状态模式是 OnReadAndWriter   右边OnCreateAndWrite

//        full Join  任意一个流有数据 能在10S内连上  状态一直更新到10S

//        左边的状态模式是 OnReadAndWrite    OnReadAndWrite


        table1.keyBy(Table1::getId)
                .process(new KeyedProcessFunction<String, Table1, Object>() {

                    private ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Long> longValueStateDescriptor = new ValueStateDescriptor<>("value",
                                Long.class);
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
//                                读和写的时候重置超时时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
//                                创建和写的时候重置超时时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        longValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                        valueState = getRuntimeContext().getState(longValueStateDescriptor);
                    }

                    @Override
                    public void processElement(Table1 value, Context ctx, Collector<Object> out) throws Exception {

                    }
                });


    }
}
