package com.zx.watermarktest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @ClassName WindowEventTime
 * @Description TODO
 * @Author Xing
 * @Date 16 14:33
 * @Version 1.0
 */
public class WindowEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 4777);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> dataDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });
//        提取时间戳
//        dataDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps());

        SingleOutputStreamOperator<WaterSensor> waterMarkDS = dataDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

//        分组
        KeyedStream<WaterSensor, String> keyedStream = waterMarkDS.keyBy(WaterSensor::getId);

        keyedStream.print("k");

        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        windowDS
                .sum("vc")
                .print("a");
//        开窗


//        聚合
        env.execute();
    }
}
