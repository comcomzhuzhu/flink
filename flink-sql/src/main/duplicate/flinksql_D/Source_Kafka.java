package flinksql_D;

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

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.connect(new Kafka()
        .version("universal")
        .topic("first")
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG, "group1"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.DOUBLE()))
        .createTemporaryTable("sensor");


        Table sensor = tableEnv.from("sensor");

        Table selectTable = sensor.groupBy($("id"))
                .aggregate($("vc").count().as("cnt"))
                .select($("id"), $("cnt"));


        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(selectTable, Row.class);

        dataStream.print();

        env.execute();
    }
}
