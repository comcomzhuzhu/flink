package com.zx.udf;


import com.zx.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName TableAggFunction_Top2
 * @Description TODO
 * @Author Xing
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
        Table select = table.groupBy($("id"))
                .flatAggregate(call(TableAggF.class, $("vc")).as("rk", "vc"))
                .select($("id"), $("rk"), $("vc"));

        tableEnvironment.toRetractStream(select, Row.class)
                .print("table API");

//        不注册使用 table API

        table.groupBy($("id"))
                .flatAggregate(call(TableAggF.class, $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute()
                .print();

//        注册使用

        tableEnvironment.createTemporarySystemFunction("aggf", new TableAggF());
        table.groupBy($("id"))
                .flatAggregate(call("aggf", $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute()
                .print();

//        无   sql 使用


        env.execute();
    }


    public static class TableAggF extends TableAggregateFunction<Tuple2<String, Double>, TableAggF.ACC> {

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ACC {
            public Double first = Double.MIN_VALUE;
            public Double second = Double.MIN_VALUE;
        }


        private int topN = 2;

        public TableAggF(int topN) {
            this.topN = topN;
        }

        @Override
        public ACC createAccumulator() {
            return new ACC();
        }

        public TableAggF() {
        }

        public void accumulate(ACC acc, Double vc) {
            if (vc > acc.getFirst()) {
                acc.setSecond(acc.getFirst());
                acc.setFirst(vc);
            } else if (vc > acc.getSecond()) {
                acc.setSecond(vc);
            }
        }

        public void emitValue(ACC acc, Collector<Tuple2<String, Double>> out) {
            out.collect(Tuple2.of("1", acc.getFirst()));

            if (acc.getSecond() != Double.MIN_VALUE) {
                out.collect(Tuple2.of("2", acc.getSecond()));
            }
        }

    }
}
