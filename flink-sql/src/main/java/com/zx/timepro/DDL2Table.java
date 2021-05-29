package com.zx.timepro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DDL2Table
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class DDL2Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(id String,ts bigint,vc double ,pt AS PROCTIME())" +
                "with( " +
                " 'connector' = 'kafka',  " +
                " 'topic' = 'first', " +
                " 'properties.bootstrap.servers' = 'zx101:9092', " +
                " 'properties.group.id' = 'testGroup', " +
                "  'format' = 'csv'  )");

        tableEnv.sqlQuery("select * from sensor ").printSchema();

    }
}
