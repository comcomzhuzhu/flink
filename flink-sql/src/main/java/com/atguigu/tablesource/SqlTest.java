package com.atguigu.tablesource;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTest {
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

        Table reTable = table.where("id='sensor_1'")
                .select("id,ts,vc");

        Table newTable = table
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));


//        动态表转换为流
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(reTable, Row.class);

        DataStream<Row> newDS = tableEnv.toAppendStream(newTable, Row.class);
        newDS.print();

        rowDataStream.print();
        env.execute();
    }
}
