package com.zx.sqlapi;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName SQL_Test
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class SQL_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) {
                        String[] strings = value.split(",");
                        return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
                    }
                });

//       获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        流转换为表
        Table table = tableEnv.fromDataStream(caseClassDS);

//      使用未注册的表
        Table reT = tableEnv.sqlQuery("select *from " + table + " where id = 'sensor1'");

//        注册表
        tableEnv.createTemporaryView("sensor", caseClassDS);

        tableEnv.createTemporaryView("sensor1", table);

        tableEnv.sqlQuery("select * from sensor where id = 'sensor1' ");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(reT, Row.class);

        rowDataStream.print();


//        rowDataStream.process()

        rowDataStream.addSink(JdbcSink.sink("insert into sensor_temp(id,temp) values(?,?) on duplicate key update id=?",
                (JdbcStatementBuilder<Row>) (preparedStatement, row) -> {
                    preparedStatement.setObject(1, row.getField(0));
//                    System.out.println(row.getField(1));
                    preparedStatement.setObject(2, row.getField(2));
                    preparedStatement.setObject(3, "sensor1");

                },
//  TODO  默认5000条数据才执行一次mysql更新操作 不可能每条数据都更新数据库
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://zx101:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));


        env.execute("test");
    }
}
