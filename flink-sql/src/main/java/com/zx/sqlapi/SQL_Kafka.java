package com.zx.sqlapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQL_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        DDL的方式从kafka读取数据
        tableEnvironment.executeSql("create table sensor(id String,ts bigint,vc double) " +
                "with (" +
                " 'connector' = 'kafka'," +
                " 'topic'='first'," +
                " 'properties.bootstrap.servers' = 'hadoop102:9092', " +
                " 'properties.group.id' = 'testGroup', " +
                " 'format' = 'csv' )");

//        过滤数据
        Table table = tableEnvironment.sqlQuery("select * from sensor where id ='sensor1'");

//        ddl的方式写入kafka
        tableEnvironment.executeSql("" +
                "create table test1(id String,ts bigint,vc double)" +
                "with (" +
                " 'connector' = 'kafka'," +
                " 'topic'='test'," +
                " 'properties.bootstrap.servers' = 'hadoop102:9092', " +
                " 'properties.group.id' = 'testGroup', " +
                " 'format' = 'json' )");


        table.executeInsert("test1");






        env.execute();
    }
}
