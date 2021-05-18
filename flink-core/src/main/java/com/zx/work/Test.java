package com.zx.work;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

public class Test {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //2.读取端口数据创建流同时提取时间戳生成Watermark,并转换为元组
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorToOneDS = streamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                String[] fields = element.split(",");
                                return Long.parseLong(fields[1]) * 1000L;
                            }
                        })).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value.split(",")[0], 1);
                    }
                });

//        sensorToOneDS.keyBy(tuple->tuple.f0)

        sensorToOneDS.print("11");
        //3.分组、开窗、聚合(使用增量聚合+窗口函数)
        KeyedStream<Tuple2<String, Integer>, String> keyBy = sensorToOneDS.keyBy(value -> value.f0);
        keyBy.print("22");

        SingleOutputStreamOperator<SensorCount> sensorCountDS = keyBy
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
                        System.out.println("何时调用？");
                        System.out.println("value1---->" + value1);
                        System.out.println("value2---->" + value2);
//  TODO 不要这样写      value1.f1 = value1.f1 + value2.f1;
//                        不能往状态里面存值  最好只能取值
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new WindowFunction<Tuple2<String, Integer>, SensorCount, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<SensorCount> out) {
                        //取出聚合后的数据
                        Tuple2<String, Integer> next = input.iterator().next();
                        //返回数据
                        out.collect(new SensorCount(window.getStart(),
                                window.getEnd(),
                                next.f0,
                                next.f1));
                    }
                });

        //4.按照窗口信息重新分组
        KeyedStream<SensorCount, Long> keyedStream = sensorCountDS.keyBy(SensorCount::getStt);

        //5.使用ProcessFunction对数据进行排序输出
        SingleOutputStreamOperator<String> result = keyedStream.process(new TopNProcessFunc(3));


        result.print();
        //7.启动
        env.execute();

    }

    public static class TopNProcessFunc extends KeyedProcessFunction<Long, SensorCount, String> {

        private int topSize;

        public TopNProcessFunc() {
        }

        public TopNProcessFunc(int topSize) {
            this.topSize = topSize;
        }

        //定义状态用于存放属于同一窗口的数据
        private ListState<SensorCount> sensorCountListState;

        @Override
        public void open(Configuration parameters) {
            sensorCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("list-state", SensorCount.class));
        }

        @Override
        public void processElement(SensorCount value, Context ctx, Collector<String> out) throws Exception {

            //将数据存放进状态
            sensorCountListState.add(value);

            //注册定时器,用于触发排序
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(value.getEdt() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的数据
            Iterator<SensorCount> iterator = sensorCountListState.get().iterator();
            ArrayList<SensorCount> sensorCounts = Lists.newArrayList(iterator);

            //排序
            sensorCounts.sort((o1, o2) -> {
                if (o1.getCount() > o2.getCount()) {
                    return 1;
                } else if (o1.getCount().equals(o2.getCount())) {
                    return 0;
                } else {
                    return -1;
                }
            });

            //输出
            StringBuilder sb = new StringBuilder("============" + (timestamp - 1000) + "============\n");

            for (int i = 0; i < Math.min(sensorCounts.size(), topSize); i++) {
                SensorCount sensorCount = sensorCounts.get(i);
                sb.append("Top")
                        .append(i + 1)
                        .append(":")
                        .append(sensorCount.getId())
                        .append("->")
                        .append(sensorCount.getCount());
                sb.append("\n");
            }

            sb.append("============")
                    .append(timestamp - 1000)
                    .append("============\n");

            //输出数据
            out.collect(sb.toString());

            //清理状态
            sensorCountListState.clear();
        }
    }
}
