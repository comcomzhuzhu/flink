package com.atguigu.sqlapi;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
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
 * @Date 2021/4/21 14:28
 * @Version 1.0
 */
public class SQL_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
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
        Table reT = tableEnv.sqlQuery("select *from" + table + "where id = 'sensor1'");

//        注册表
        tableEnv.createTemporaryView("sensor", caseClassDS);

        tableEnv.createTemporaryView("sensor1", table);

        tableEnv.sqlQuery("select * from sensor where id = 'sensor1' ");

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(reT, Row.class);

        rowDataStream.print();

        env.execute();
    }
}
