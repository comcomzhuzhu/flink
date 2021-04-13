package com.atguigu.watermarktest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName WaterMarkTest
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/13 23:35
 * @Version 1.0
 */
public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

//   TODO       过期了  对有序数据的watermark   -1  watermark的当前时间戳是看到的数据时间戳
//        caseDS.assignTimestampsAndWatermarks(new AscendingTimestampsExtractor<>)
//         有界乱序 时间戳提取器     参数 最大延迟时间  最大乱序程度  就是第一层保障
//        周期性 生成waterMark 数据插入数据流 不是一个数据生成一个
//         必须保持一个看到的最大时间戳  而不是直接提取时间戳
        caseDS.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(100L)) {
            @Override
            public long extractTimestamp(SensorReading element) {
//                必须毫秒数
                return element.getTimeStamp() *1000;
            }
        });



        env.execute();
    }
}
