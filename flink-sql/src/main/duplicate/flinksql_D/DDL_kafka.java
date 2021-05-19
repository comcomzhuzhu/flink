package flinksql_D;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DDL_kafka
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class DDL_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment.executeSql("create table source " +
                "(id String,ts bigint,vc double)  with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'first'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                " 'properties.group.id' = 'testGroup', " +
                " 'format' = 'csv' " +
                ")");

        tableEnvironment.executeSql("create table sink " +
                "(id String,ts bigint,vc double)  with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'test'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                " 'properties.group.id' = 'testGroup', " +
                " 'format' = 'json' " +
                ")");

        Table table = tableEnvironment.sqlQuery("select * from source where id = 'sensor_1' ");

        table.executeInsert("sink");

        env.execute();
    }
}
