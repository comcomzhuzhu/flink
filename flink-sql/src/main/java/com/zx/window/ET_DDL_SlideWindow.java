package com.zx.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName ET_DDL_SlideWindow
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class ET_DDL_SlideWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table sensor(id String,ts bigint,vc double," +
                "rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts))," +
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND )" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'first'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        Table table = tableEnvironment.sqlQuery("select *from sensor");

        table
                .window(Slide.over(lit(10).second()).every(lit(5).second()).on($("rt")).as("w"))
                .groupBy($("id"),$("w"))
//                .aggregate($("id").count().as("cnt"))
//                .select($("id"),$("cnt"))
                .select($("id"),$("vc").sum())
                .execute()
                .print();



        env.execute();
    }
}
