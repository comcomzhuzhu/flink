package com.atguigu.udf;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName TableFunction_SplitString
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/22 15:04
 * @Version 1.0
 */
public class TableFunction_SplitString {
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


        table.joinLateral(call(MyTableFunction.class,$("id")).as("word", "length"))
                .select($("id"),$("word"),$("length"))
                .execute().print();

        env.execute();
    }


    public static class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {
        public MyTableFunction() {
        }

        private String separator = ",";

        public MyTableFunction(String split) {
            this.separator = split;
        }

        public void eval(String str) {
            String[] split = str.split(separator);
            for (String s : split) {
                collect(Tuple2.of(s, s.length()));
            }


        }

    }


}
