package com.zx.cdctest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSQLCdc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        开启 非对齐barrier  1.13后 超时自动非对齐
        env.isUnalignedCheckpointsEnabled();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table user_info(" +
                "id int," +
                "name string," +
                "phone_num string)" +
                "with(" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'zx101', " +
                " 'port' = '3306', " +
                " 'username' = 'root'," +
                " 'password' = '123456'," +
                " 'database-name' = 'gmall-flink'," +
                " 'table-name' = 'z_user_info' )");


        tableEnvironment.sqlQuery("select *from user_info")
                .execute()
                .print();


    }
}
