package com.atguigu.tablesource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Source_File
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/21 10:32
 * @Version 1.0
 */
public class Source_File {
    public static void main(String[] args) throws Exception {
//       获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

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
        Table sensor = tableEnvironment.from("sensor");

//        查询
        Table select = sensor.where($("id").isEqual("sensor1"))
                .select($("id"), $("vc"), $("ts"));

//        转换为流
        DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(select, Row.class);

        rowDataStream.print();
//        启动
        env.execute();
    }
}
