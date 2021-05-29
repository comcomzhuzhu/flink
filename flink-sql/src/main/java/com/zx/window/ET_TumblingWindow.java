package com.zx.window;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class ET_TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        StreamTableEnvironment.create(env)
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        Configuration configuration = tableEnvironment.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled", "true");
// 批量输出的间隔时间
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
// 防止OOM设置每个批次最多缓存数据的条数，可以设为2万条
        configuration.setString("table.exec.mini-batch.size", "20000");

        // 开启LocalGlobal
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");

        // 设置参数：
//         开启Split Distinct
        configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
//         第一层打散的bucket数目
        configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");

//         设置参数：
//         默认10000条，调整TopN cahce到20万，那么理论命中率能达200000*50/100/100000 = 100%
        configuration.setString("table.exec.topn.cache-size", "200000");

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8888);

        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor> withWMDS = caseClassDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));


        Table table = tableEnvironment.fromDataStream(withWMDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());


        table.window(Tumble.over(lit(5).second())
                .on($("rt")).as("w"))
                .groupBy($("id"), $("w"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"))
                .execute()
                .print();


        env.execute();
    }
}
