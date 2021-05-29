package com.zx.watermarktest;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * In Flink 1.12 the default stream time characteristic has been changed to {@link
 *        TimeCharacteristic#EventTime}
 *TODO
 * 基于事件语义的窗口已经和现实的时间无关了只跟数据的时间戳有关
 * 必须等上游的所有分区的watermark 的最小值都 达到了 窗口关闭的条件才会触发计算
 */

public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");
        //     public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
//		this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
//		if (characteristic == TimeCharacteristic.ProcessingTime) {
//			getConfig().setAutoWatermarkInterval(0);
//		} else {
//			getConfig().setAutoWatermarkInterval(200);
//		}
//	}
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        DataStreamSource<String> dss = env.socketTextStream("zx101", 1208);
        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        })

//   TODO   对有序数据的watermark   -1  watermark的当前时间戳是看到的数据时间戳
//        caseDS.assignTimestampsAndWatermarks(new AscendingTimestampsExtractor<>)
//         有界乱序 时间戳提取器     参数 最大延迟时间  最大乱序程度  就是第一层保障
//        周期性 生成waterMark 数据插入数据流 不是一个数据生成一个
//         必须保持一个看到的最大时间戳  而不是直接提取时间戳
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
//                必须毫秒数
                return element.getTimeStamp() *1000L;
            }
        });
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};
//      基于事件时间的开窗聚合  统计15S内温度的最小值
        SingleOutputStreamOperator<SensorReading> re0DS = caseDS.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)

//      迟到的数据就算不是最小值也更新了 最小值
                .minBy("temperature");
        re0DS.getSideOutput(outputTag).print("late");
//      需要数据的时间戳推移15S才有输出
//      为什么有多个输出 因为keyBy了 每个key一个窗口
//      195-210的窗口  在 时间戳到了212时输出了结果关闭了窗口



//  根据一个算法 算出每个窗口的开始时间是 窗口的 整数倍

        /**
         * Method to get the window start for a timestamp.
         *
         * @param timestamp epoch millisecond to get the window start.
         * @param offset The offset which window start would be shifted by.
         * @param windowSize The size of the generated windows.
         * @return window start
//       */
//        public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
//            return timestamp - (timestamp - offset + windowSize) % windowSize;
//        }
//          long start = TimeWindow.getWindowStartWithOffset(timestamp, (globalOffset + staggerOffset) % size, size);
//			return Collections.singletonList(new TimeWindow(start, start + size));


        re0DS.print("min temp");

        env.execute();
    }
}
