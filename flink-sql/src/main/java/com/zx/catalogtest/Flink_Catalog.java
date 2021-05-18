package com.zx.catalogtest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink_Catalog {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        String name = "myhive";
        String default_database = "default";
        String hiveConfDir = "flink-sql/input/hiveconf";
//        创建hive catalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, default_database, hiveConfDir);

        tableEnvironment.registerCatalog(name, hiveCatalog);

        tableEnvironment.useCatalog(name);
        tableEnvironment.sqlQuery("select * from student")
                .execute()
                .print();


        env.execute();
    }
}
