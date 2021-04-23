package com.atguigu.window;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName OverWindow_Unbounded
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/22 11:26
 * @Version 1.0
 */
public class OverWindow_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<String> withWMDS = socketTextStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.valueOf(split[1]) * 1000L;
                    }
                }));

        SingleOutputStreamOperator<WaterSensor> dataDS = withWMDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });


        Table table = tableEnvironment.fromDataStream(dataDS,
                $("id"),
                $("ts").rowtime(),
                $("vc"));


        table.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w1"))
                .select($("id"), $("ts"), $("vc").sum().over($("w1")).as("sumVC"))
                .execute()
                .print();


        table.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_RANGE).as("w"))
                .select($("id"), $("ts"), $("vc").sum().over($("w")).as("sum"))
                .execute()
                .print();


        env.execute();
    }
}