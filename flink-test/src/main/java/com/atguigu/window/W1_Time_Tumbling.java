package com.atguigu.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName W1_Time_Tumbling
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/12 23:14
 * @Version 1.0
 */
public class W1_Time_Tumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fileds = value.split(",");
            return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
        });

//        window All 会将所有数据放入一个窗口
        caseDS.keyBy("id");

//                .countWindow(10,5);  底层传入了一个全局窗口 所以就这么写
//                .countWindow(10);
//                .window(EventTimeSessionWindows.withGap(Time.milliseconds(1000)));
//                .timeWindow(Time.seconds(15)); //底层是 of方法

//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        env.execute();
    }
}
