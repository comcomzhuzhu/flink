package flinksql_D;

import org.apache.flink.api.java.tuple.Tuple2;
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
 * @Date 21 11:38
 * @Version 1.0
 */
public class Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new FileSystem()
        .path("flink-test/input/sensor.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor");


        Table sensor = tableEnv.from("sensor");

        Table selectReTable = sensor
//                .select($("id"));
                .groupBy($("id"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"));

        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnv.toRetractStream(selectReTable, Row.class);

        resultDS.print().setParallelism(1);


        env.execute();
    }
}
