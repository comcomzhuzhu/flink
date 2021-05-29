package com.zx.timepro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DDL2Table_ET
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class DDL2Table_ET {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table test1 (" +
                "id String,ts bigint,vc double," +
                "rt As TO_TIMESTAMP(FROM_UNIXTIME(ts))," +
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND )" +
                "with(   " +
                "'connector.type' = 'kafka'," +
                "'connector.version' = 'universal'," +
                "'connector.topic' = 'test'," +
                "'connector.properties.bootstrap.servers' = 'zx101:9092'," +
                "'connector.properties.group.id' = 'bigdata1109'," +
                "'format.type' = 'json'" +
                ")");

        tableEnvironment.sqlQuery("select *from test1").printSchema();


    }
}
