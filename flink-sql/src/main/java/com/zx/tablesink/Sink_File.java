package com.zx.tablesink;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Sink_File
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Sink_File {
    public static void main(String[] args) throws Exception {
        //       获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8888);

        SingleOutputStreamOperator<WaterSensor> dataDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

//       使用连接器的方式读取文本数据
        tableEnvironment.connect(new FileSystem()
                .path("flink-test/input/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");

//        使用table API
//        将读取的表转换为动态表
        Table table = tableEnvironment.fromDataStream(dataDS);

//        查询
        Table select = table.where($("id").isEqual("sensor1"))
                .select($("id"), $("vc"), $("ts"));

//        file 只能sink 追加流
//        table.groupBy($("id"),$("ts"))
//                .aggregate($("vc").sum().as("vcSum"))
//                .select($("id"),$("ts"),$("vc"));

//        将数据通过连接器写入文件
        select.executeInsert("sensor");

        tableEnvironment.insertInto("sensor", select);

//        启动
        env.execute();
    }
}
