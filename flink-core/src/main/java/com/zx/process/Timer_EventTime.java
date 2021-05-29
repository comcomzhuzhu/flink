package com.zx.process;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 200ms的间隔 生成watermark
 * 当一条携带时间戳的数据进入系统
 * 那么系统将在200ms内生成由这个数据的时间戳对应的watermark
 * 例如 时间戳为2000
 * 生成1999watermark
 *
 * 只能“看到”  也就是获取到上一条数据生成的watermark
 * 本条数据触发的方法 还没有生成watermark
 *
 */
public class Timer_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> dataDS = env.socketTextStream("zx101", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
                    }
                });

        dataDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) {
                        out.collect(value);

                        TimerService timerService = ctx.timerService();
//                      此处获得了 周期型 只能获取到上一个数据生成的watermark
//                      生成的上一个数据的时间戳 -延迟时间生成的watermark
                        long watermark = timerService.currentWatermark();

                        timerService.registerEventTimeTimer(watermark + 5000L - 1);
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        System.out.println("定时器触发");
                    }
                });

        env.execute();
    }
}
