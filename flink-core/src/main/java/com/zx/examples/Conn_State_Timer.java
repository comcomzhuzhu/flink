package com.zx.examples;

import com.zx.apitest.beans.OrderEvent;
import com.zx.apitest.beans.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

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
        }).filter(data -> data.getTxId() != null).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
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

        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = ordercaseDS.connect(txcaseDS).keyBy(OrderEvent::getTxId, TxEvent::getTxId);

        ordercaseDS.keyBy(OrderEvent::getTxId)
                .connect(txcaseDS.keyBy(TxEvent::getTxId))
                .process(new MyKeyedProcessFuncJoin())
                .print("work");


        env.execute();
    }


    public static class MyCoProFunc extends CoProcessFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        private MapState<Long, List<OrderEvent>> orderEventState;
        private MapState<Long, List<TxEvent>> txEventState;
        private TypeSerializer<OrderEvent> OrderEventSer;
        private TypeSerializer<TxEvent> TxEventSer;

        private long bound;

        public MyCoProFunc() {
        }

        //         this is seconds
        public MyCoProFunc(long bound) {
            this.bound = bound;
        }


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
                    new MapStateDescriptor<>("tx"
                            , LongSerializer.INSTANCE, new ListSerializer<>(txEventTypeSerializer)));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {

            Iterable<List<TxEvent>> values = txEventState.values();
            for (List<TxEvent> txEvents : values) {
                for (TxEvent txEvent : txEvents) {
                    if (txEvent.getTxId().equals(value.getTxId())) {
                        out.collect(Tuple2.of(value, txEvent));
                    }
                }
            }


        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            TimerService timerService = ctx.timerService();
            long watermark = timerService.currentWatermark();

            timerService.registerEventTimeTimer(watermark + bound * 1000L);


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
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            Iterable<Long> keys = txEventState.keys();
            for (Long key : keys) {
            }
        }
    }

    public static class MyKeyedProcessFuncJoin extends KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>> {
        //        存放两个流的数据
        private ValueState<OrderEvent> orderEventValueState;
        private ValueState<TxEvent> txEventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class)
            );
            txEventValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<TxEvent>("txEvent", TxEvent.class)
            );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            orderEventValueState.clear();
            txEventValueState.clear();
        }

        @Override
        public void processElement1(OrderEvent orderEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
//            提取数据
            TxEvent txEvent = txEventValueState.value();
//            判断到账数据是否到达
            if (txEvent != null) {
//          如果已到达  连接 写出
                out.collect(Tuple2.of(orderEvent, txEvent));
//                清理状态
                txEventValueState.clear();
            } else {
                orderEventValueState.update(orderEvent);
//                注册定时器 触发删除状态操作
                TimerService timerService = ctx.timerService();
//                提取数据中的时间
                long ts = orderEvent.getEventTime() * 1000L;
                timerService.registerEventTimeTimer(ts + 10000L);
            }
        }

        @Override
        public void processElement2(TxEvent txEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            OrderEvent orderEvent = orderEventValueState.value();

//            判断支付数据是否到达
            if (orderEvent != null) {
                out.collect(Tuple2.of(orderEvent, txEvent));
            } else {
                txEventValueState.update(txEvent);
//                注册定时器用于触发删除状态操作
                TimerService timerService = ctx.timerService();
                long ts = txEvent.getEventTime() * 1000L;
                timerService.registerEventTimeTimer(ts + 10000L);
            }
        }
    }

}