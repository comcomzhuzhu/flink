package com.zx.window;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName PT_Stream_Tumbling
 * @Description TODO
 * @Author Xing
 * 22 10:11
 * @Version 1.0
 */
public class PT_Stream_Tumbling {
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

        SingleOutputStreamOperator<WaterSensor> caseClassDS = withWMDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        Table table = tableEnvironment.fromDataStream(caseClassDS,
                $("id"),
                $("ts").rowtime(),
                $("vc"));

        Table result = table.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start().as("Wstart"),
                        $("w").end().as("Wend"), $("vc").sum().as("sum"));


        table.window(Tumble.over(lit(10).seconds()).on($("ts")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("w").start().as("windowS"),
                        $("w").end().as("windowE"),
                        $("vc").sum().as("sum"))
                .execute().print();


        tableEnvironment.toRetractStream(result, Row.class)
                .print();


        env.execute();

    }
}
