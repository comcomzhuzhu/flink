package com.atguigu.udf;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName ScalarFunction_HashCode
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/22 14:23
 * @Version 1.0
 */
public class ScalarFunction_HashCode {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> textFile = env.readTextFile("flink-core/input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> dataDS = textFile.map((MapFunction<String, WaterSensor>) value -> {
            String[] strings = value.split(",");
            return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
        });


        Table table = tableEnvironment.fromDataStream(dataDS,
                $("id"),
                $("ts"),
                $("vc"));

        tableEnvironment.createTemporaryView("sensor", table);


        MyHashCode myHashCode = new MyHashCode(17);
        tableEnvironment.createTemporarySystemFunction("myhash", MyHashCode.class);

        table.select("id, ts, myhash(id)")
                .execute()
                .print();

        table.select($("id"),call("myhash", $("id")))
                .execute().print();

        tableEnvironment.sqlQuery("select id,myhash(id)" +
                "from sensor").execute().print();


        env.execute();
    }

    public static class MyHashCode extends ScalarFunction {

        public int eval(String string) {
            return string.hashCode() * factor;
        }

        public MyHashCode() {
        }

        private int factor = 13;

        public MyHashCode(int factor) {
            this.factor = factor;
        }

    }


}
