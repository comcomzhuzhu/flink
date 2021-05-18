package com.zx.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName PT_DDL_TumblingWindow
 * @Description TODO
 * @Author Xing
 * @Date 21 20:35
 * @Version 1.0
 */
public class PT_DDL_TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table sensor(id String,ts bigint,vc double," +
                "pt AS PROCTIME())" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'first'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'scan.startup.mode' = 'earliest-offset'," +
                "'format' = 'csv'" +
                ")");

        Table table = tableEnvironment.sqlQuery("select * from sensor");

        Table result = table.window(Tumble.over(lit(10).seconds()).on($("pt")).as("w"))
                .groupBy($("w"), $("id"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"));

        tableEnvironment.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
