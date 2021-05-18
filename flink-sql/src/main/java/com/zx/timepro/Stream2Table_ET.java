package com.zx.timepro;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Stream2Table_ET
 * @Description TODO
 * @Author Xing
 * @Date 21 20:58
 * @Version 1.0
 */
public class Stream2Table_ET {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
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

        Table table = tableEnv.fromDataStream(withWMDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());

        table.printSchema();


        env.execute();

    }
}
