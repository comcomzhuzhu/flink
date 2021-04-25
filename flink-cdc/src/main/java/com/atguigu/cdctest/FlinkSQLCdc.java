package com.atguigu.cdctest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkSQLCdc
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/23 20:27
 * @Version 1.0
 */
public class FlinkSQLCdc {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.isUnalignedCheckpointsEnabled();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table user_info(" +
                "id int," +
                "name string," +
                "phone_num string)" +
                "with(" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = 'hadoop102', " +
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
