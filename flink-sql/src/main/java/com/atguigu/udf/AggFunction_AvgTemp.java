package com.atguigu.udf;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class AggFunction_AvgTemp {
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
//        table api 不注册
        table.groupBy($("id"))
                .aggregate(call(AvgTemp.class, $("vc")).as("avg"))
                .select($("id"), $("avg"))
                .execute()
                .print();

//        table  注册使用
        tableEnvironment.createTemporarySystemFunction("myavg", AvgTemp.class);


        tableEnvironment.sqlQuery("select id,myavg(vc) from " + table+" group by id");


        env.execute();
    }


    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f1 / accumulator.f0;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0.0, 0);
        }


        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            accumulator.f0 += temp;
            accumulator.f1 += 1;
        }

    }


}