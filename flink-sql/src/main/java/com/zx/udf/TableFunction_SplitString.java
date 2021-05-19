package com.zx.udf;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName TableFunction_SplitString
 * @Description TODO
 * @Author Xing
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

//        不注册  table api
        table.joinLateral(call(MyUDTF.class, $("id")))
                .select($("id"), $("word"), $("length"))
                .execute()
                .print();


        table.joinLateral(call(MyTableFunction.class, $("id")).as("word", "length"))
                .select($("id"), $("word"), $("length"))
                .execute().print();


        tableEnvironment.createTemporarySystemFunction("mySplit", MyUDTF.class);

//        sql 注册使用
        tableEnvironment.sqlQuery("select id,word,length from " + table + "" +
                ",lateral table(mySplit(id))")
                .execute()
                .print();


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

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class MyUDTF extends TableFunction<Row> {
        public void eval(String string) {
            String[] strings = string.split("_");
            for (String word : strings) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}
