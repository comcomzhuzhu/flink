package com.atguigu.sqlapi;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DS2SQL_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        获取数据流
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        tableEnv.createTemporaryView("sensor", caseClassDS);

//        查询
        Table resultTable = tableEnv.sqlQuery("select id,count(id) from sensor group by id");


        TableResult tableResult = tableEnv.executeSql("select id,count(id) from sensor group by id");

        tableResult.print();
//        table result 阻塞了 下面的代码 都不执行了
        System.out.println("这里及下方的代码被阻塞了不会执行");

//        转换为撤回流
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute();
    }
}
