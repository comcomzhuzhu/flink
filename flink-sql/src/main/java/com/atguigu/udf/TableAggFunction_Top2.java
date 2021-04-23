package com.atguigu.udf;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName TableAggFunction_Top2
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/22 16:01
 * @Version 1.0
 */
public class TableAggFunction_Top2 {
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

//        不注册使用 table api

        tableEnvironment.createTemporarySystemFunction("aggf", new TableAggF(2));


        table.groupBy($("id"))
                .flatAggregate(call("aggf", $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
                .execute()
                .print();

//        注册使用



//        sql 使用


        env.execute();
    }


    public static class TableAggF extends TableAggregateFunction<Tuple2<Double, Integer>, ArrayList<Double>> {

        private int topN = 2;

        public TableAggF(int topN) {
            this.topN = topN;
        }

        @Override
        public ArrayList<Double> createAccumulator() {
            return new ArrayList<Double>(topN);
        }

        public TableAggF() {
        }

        public void accumulate(ArrayList<Double> acc, Double vc) {
            if (vc > acc.get(0)) {
                acc.set(1, acc.get(0));
                acc.set(0, vc);
            } else if (vc > acc.get(1)) {
                acc.set(1, vc);
            }
        }

        public void emitValue(ArrayList<Double> acc, Collector<Tuple2<Double, Integer>> out) {
            for (int i = 0; i < Math.min(topN, acc.size()); i++) {
                out.collect(Tuple2.of(acc.get(i), i + 1));
            }
        }

    }
}
