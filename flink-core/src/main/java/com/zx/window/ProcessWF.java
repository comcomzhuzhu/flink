package com.zx.window;

import com.zx.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName ProcessWF
 * @Description TODO
 * @Author Xing
 * 13 18:34
 * @Version 1.0
 */
public class ProcessWF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);

        SingleOutputStreamOperator<SensorReading> caseDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        caseDS.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) {
                return value.getId();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                process 的全窗口函数
//                .process(new ProcessWindowFunction<SensorReading, Tuple3<String,Long,Long>, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Long>> out) {
//                    }
//                })
// TODO    apply IN, OUT, KEY, W 四个泛型 key是分组后的key  W是当前window
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Long>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Long>> out) {
                        long count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(Tuple3.of(s,count,window.getEnd()));
                    }
                }).print("processWindowFunction");
        env.execute();
    }
}
