package com.atguigu.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/* TODO 对于迟到和乱序的数据
 *  需要事件时间语义waterMark  需要allowedLateness  需要侧输出流 三重保障
 *  对与乱序数据 准时输出窗口结果 再在allowedLateness时间内 更新结果
 *  之后做批流合并处理 得到准确的结果
 */
public class Other_API {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        SingleOutputStreamOperator<SensorReading> re0DS = caseDS.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) {
                return value.getId();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(15)))
//                 事件时间语义
//TODO                .trigger()
//                    .evictor()
//      允许延迟的时间
//      如果允许迟到，到时间了先输出结果 但是不关闭窗口
//      如果有迟到的数据过来,在允许迟到的时间间隔内 在之前的结果上修改之前的结果
//      到了窗口时间+允许延迟时间后窗口会关闭
//      Setting an allowed lateness is only valid for event-time windows.
                .allowedLateness(Time.seconds(1))
//      如果在窗口关闭之后 还有迟到的数据, 扔入侧输出流
                .sideOutputLateData(outputTag)
                .sum("temperature");
//      一分钟之后的迟到数据会在原流上re0输出
//  TODO  做完窗口计算之后 在结果流中还能获取之后的侧输出流中的迟到数据
        re0DS.getSideOutput(outputTag).print("late");
//          之后批处理  处理两条流的数据  实现了两套架构的结果  流处理快速输出结果 +侧输出流 批处理

        env.execute();
    }
}
