package flinksql_D;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DDL_Table
 * @Description TODO
 * @Author Xing
 * @Date 21 20:54
 * @Version 1.0
 */
public class DDL_Table {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        TableResult tableResult = tableEnvironment.executeSql("create table aa(" +
                "id String,ts bigint, vc double ,pt AS PROCTIME() )" +
                "'connector' = 'kafka'," +
                "'topic' = 'first'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'format' = 'csv'");

        tableEnvironment.sqlQuery("select * from aa").printSchema();


        env.execute();
    }
}
