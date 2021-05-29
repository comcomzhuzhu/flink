package com.zx.duplicate;

import com.zx.apitest.beans.OrderEvent;
import com.zx.apitest.beans.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Conn_Timer_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> orderDS = env.socketTextStream("zx101", 7777);
        DataStreamSource<String> txDS = env.socketTextStream("zx101", 8888);

        SingleOutputStreamOperator<OrderEvent> ordercaseDS = orderDS.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) {
                String[] strings = value.split(",");
                return new OrderEvent(Long.valueOf(strings[0]), strings[1],
                        strings[2], Long.valueOf(strings[3]));
            }
        }).filter(data -> data.getTxId() != null)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        }));

        SingleOutputStreamOperator<TxEvent> txcaseDS = txDS.map(line -> {
                    String[] strings = line.split(",");
                    return new TxEvent(strings[0], strings[1], Long.valueOf(strings[2]));
                }
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<TxEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
                    @Override
                    public long extractTimestamp(TxEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        ordercaseDS.keyBy(OrderEvent::getTxId)
                .connect(txcaseDS.keyBy(TxEvent::getTxId))
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {
                    ValueState<OrderEvent> orderEventValueState;
                    ValueState<TxEvent> txEventValueState;

                    @Override
                    public void open(Configuration parameters) {
                        orderEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("order", OrderEvent.class));
                        txEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("tx", TxEvent.class));
                    }

                    @Override
                    public void processElement1(OrderEvent orderEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                        TimerService timerService = ctx.timerService();

                        if (txEventValueState.value() != null) {
                            out.collect(Tuple2.of(orderEvent, txEventValueState.value()));
                            txEventValueState.clear();
                        } else {
                            orderEventValueState.update(orderEvent);
                            timerService.registerEventTimeTimer(orderEvent.getEventTime() + 10000L);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent txEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

                        if (orderEventValueState.value() != null) {
                            out.collect(Tuple2.of(orderEventValueState.value(), txEvent));
                            orderEventValueState.clear();
                        } else {
                            txEventValueState.update(txEvent);
                            ctx.timerService().registerEventTimeTimer(txEvent.getEventTime() + 10000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                        orderEventValueState.clear();
                        txEventValueState.clear();
                    }
                })
                .print();


        env.execute();
    }


}
