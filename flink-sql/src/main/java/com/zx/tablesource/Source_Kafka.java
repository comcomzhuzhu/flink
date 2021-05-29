package com.zx.tablesource;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName Source_Kafka
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

//        使用连接器的方式从kafka读数据
        tableEnvironment.connect(new Kafka()
                .topic("first")
                .startFromLatest()
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "zx101:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "test")
        .version("universal"))
                .withFormat(new Csv())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");


        Table sensor = tableEnvironment.from("sensor");


        Table result = sensor.where($("id").isEqual("sensor1"))
                .select($("id"), $("cnt"));

        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnvironment.toRetractStream(result, Row.class);


        dataStream.print();

        env.execute();
    }
}
