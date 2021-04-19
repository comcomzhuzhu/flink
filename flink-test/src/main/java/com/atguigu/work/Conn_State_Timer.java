package com.atguigu.work;

import com.atguigu.apitest.beans.OrderEvent;
import com.atguigu.apitest.beans.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName Conn_State_Timer
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/19 14:56
 * @Version 1.0
 */
public class Conn_State_Timer {
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

        SingleOutputStreamOperator<TxEvent> txcaseDS = txDS.map(line -> {
                    String[] strings = line.split(",");
                    return new TxEvent(strings[0], strings[1], Long.valueOf(strings[2]));
                }
        );


        ordercaseDS.keyBy(OrderEvent::getTxId)
                .connect(txcaseDS.keyBy(TxEvent::getTxId))
                .process(new MyCoProFunc())
                .print("work");


        env.execute();
    }


    public static class MyCoProFunc extends CoProcessFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        private MapState<Long, List<OrderEvent>> orderEventState;
        private MapState<Long, List<TxEvent>> txEventState;
        private TypeSerializer<OrderEvent> OrderEventSer;
        private TypeSerializer<TxEvent> TxEventSer;

        @Override
        public void open(Configuration parameters) {
            TypeSerializer<OrderEvent> orderEventTypeSerializer = Preconditions.checkNotNull(OrderEventSer);
            orderEventState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("order",
                            LongSerializer.INSTANCE,
                            new ListSerializer<>(orderEventTypeSerializer))
            );
            TypeSerializer<TxEvent> txEventTypeSerializer = Preconditions.checkNotNull(TxEventSer);

            txEventState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "tx"
                            , LongSerializer.INSTANCE, new ListSerializer<>(txEventTypeSerializer))
            );
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            TimerService timerService = ctx.timerService();
            long watermark = timerService.currentWatermark();


            Iterable<List<TxEvent>> values = txEventState.values();
            for (List<TxEvent> txEvents : values) {
                for (TxEvent txEvent : txEvents) {
                    if (txEvent.getTxId().equals(value.getTxId())) {
                        out.collect(Tuple2.of(value, txEvent));
                    }
                }
            }

            if (orderEventState.contains(value.getEventTime())) {
                List<OrderEvent> orderEvents = orderEventState.get(value.getEventTime());
                orderEvents.add(value);
            } else {
                ArrayList<OrderEvent> orderEvents = new ArrayList<>();
                orderEvents.add(value);
                orderEventState.put(value.getEventTime(), orderEvents);
            }
        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            Iterable<List<OrderEvent>> values = orderEventState.values();
            for (List<OrderEvent> orderEvents : values) {
                for (OrderEvent orderEvent : orderEvents) {
                    if (orderEvent.getTxId().equals(value.getTxId())) {
                        out.collect(Tuple2.of(orderEvent, value));
                    }
                }
            }

            if (txEventState.contains(value.getEventTime())) {
                List<TxEvent> txEvents = txEventState.get(value.getEventTime());
                txEvents.add(value);
            } else {
                ArrayList<TxEvent> txEvents = new ArrayList<>();
                txEvents.add(value);
                txEventState.put(value.getEventTime(), txEvents);
            }


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) {


        }
    }
}

