package com.zx.window;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Window_EventTimeWindow
 * @Description TODO
 * @Author Xing
 * 13 21:27
 * @Version 1.0
 * 1.12 默认事件时间
 */
public class Window_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        env.execute();
    }
}
