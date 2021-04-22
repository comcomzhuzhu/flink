package com.atguigu.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName SQL_GroupWindow
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/22 11:42
 * @Version 1.0
 */
public class SQL_GroupWindow {
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


        tableEnvironment.sqlQuery(
                "SELECT id," +
                        "TUMBLE_START(rt ,INTERVAL '1' minute) as wStart," +
                        "TUMBLE_END(rt,INTERVAL '1' minute) as wEnd," +
                        " SUM(vc) sum_Vc " +
                        "FROM sensor " +
                        "GROUP BY TUMBLE(rt, INTERVAL '1' minute),id"
        )
                .execute().print();

//        tableEnvironment
//                .sqlQuery(
//                        "SELECT id, " +
//                                "  TUMBLE_START(rt, INTERVAL '1' minute) as wStart,  " +
//                                "  TUMBLE_END(rt, INTERVAL '1' minute) as wEnd,  " +
//                                "  SUM(vc) sum_vc " +
//                                "FROM sensor " +
//                                "GROUP BY TUMBLE(rt, INTERVAL '1' minute), id"
//                )
//                .execute()
//                .print();


        env.execute();
    }
}
