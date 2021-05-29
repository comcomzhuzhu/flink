package com.zx.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Sink_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("", 8841);

        socketDS.addSink(JdbcSink.sink("insert into wc_1 values(?,?)on duplicate key update ct=?",
                new JdbcStatementBuilder<String>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, String s) throws SQLException {
                        preparedStatement.setString(1,"");
                    }
                },
//  TODO  默认5000条数据才执行一次mysql更新操作 不可能每条数据都更新数据库
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName("com.mysql.jdbc.Driver")
        .withUrl("jdbc://mysql://zx101:3306/test?useSSL=false")
        .withUsername("root")
        .withPassword("123456").build()));


        env.execute();
    }
}
