package com.zx.tablesource;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName SqlAgg
 * @Description TODO
 * @Author Xing
 * 21 10:10
 * @Version 1.0
 */
public class SqlAgg {
//     如果更新了原来的数据  需要使用撤回流
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

        Table countTable = table.groupBy("id")
                .select("id,id.count");

        Table newT = table.groupBy($("id"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"));


        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(countTable, Row.class);

        DataStream<Tuple2<Boolean, Row>> newDS = tableEnv.toRetractStream(newT, Row.class);
        newDS.print();

        retractStream.print();
        env.execute();
    }
}
