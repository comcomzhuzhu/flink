package com.zx.window;


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
 * @ClassName SQL_OverWindow
 * @Description TODO
 * @Author Xing
 * @Date 23 11:12
 * @Version 1.0
 */
public class SQL_OverWindow {
    public static void main(String[] args) {
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
                $("ts"),
                $("vc"),
                $("pt").proctime());


        tableEnvironment.createTemporaryView("sensor", table);

        tableEnvironment.sqlQuery("select " +
                " id," +
                " rk,vc " +
                " from" +
                " ( select" +
                "   id,vc, " +
                "   ROW_NUMBER() OVER(partition by id order by vc)  rk" +
                "   from sensor)t" +
                "   where rk <=3 ")
                .execute()


                .print();

    }
}