package com.zx.tablesink;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });


//        使用连接器的方式 sink 到kafka
        tableEnvironment.connect(new Kafka()
                .topic("first")
                .startFromLatest()
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .version("universal"))
                .withFormat(new Json())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("vc", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");


        Table sensor = tableEnvironment.fromDataStream(caseClassDS);

        Table result = sensor.where($("id").isEqual("sensor1"))
                .select($("id"), $("vc"), $("ts"));

        result.executeInsert("sensor");

        env.execute();
    }
}
