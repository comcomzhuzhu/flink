package com.atguigu.process;

import com.atguigu.apitest.beans.OrderEvent;
import com.atguigu.apitest.beans.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName Process_Join
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/17 16:35
 * @Version 1.0
 */
public class Process_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> orderDS = env.readTextFile("flink-test/input/OrderLog.csv");
        DataStreamSource<String> txDS = env.readTextFile("flink-test/input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> ordercaseDS = orderDS.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) {
                String[] strings = value.split(",");
                return new OrderEvent(Long.valueOf(strings[0]), strings[1],
                        strings[2], Long.valueOf(strings[3]));
            }
        }).filter(data -> data.getTxId() != null);

        SingleOutputStreamOperator<TxEvent> txcaseDS = txDS.map(line ->
                {
                    String[] strings = line.split(",");
                    return new TxEvent(strings[0], strings[1], Long.valueOf(strings[2]));
                }
        );


        ordercaseDS.keyBy(OrderEvent::getTxId)
                .intervalJoin(txcaseDS.keyBy(TxEvent::getTxId))
                .between(Time.seconds(-10),Time.seconds(10) )
//                .lowerBoundExclusive()   去除最前一毫秒  (  ]
//                .upperBoundExclusive()
                .process(new ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent,TxEvent>>() {
                    @Override
                    public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                        out.collect(Tuple2.of(left,right ));
                    }
                }).print("join");

        env.execute();
    }
}
