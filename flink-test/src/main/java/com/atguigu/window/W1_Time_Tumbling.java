package com.atguigu.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

public class W1_Time_Tumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

//        window All 会将所有数据放入一个窗口
        caseDS.keyBy("id")
        .window(SlidingProcessingTimeWindows.of(Time.seconds(50), Time.seconds(5)))
        .sum("temperature").print("a");


//TODO
//              .countWindow(10,5); 滑动计数 底层传入了一个全局窗口 所以就这么写
//  计数        .countWindow(10);  滚动计数
//  会话        .window(EventTimeSessionWindows.withGap(Time.milliseconds(1000)));
//              .timeWindow(Time.seconds(15));   底层是window的of方法
//              .windowAll() 传入同一个实例
//  滚动        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        env.execute();
    }
}
