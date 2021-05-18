package com.zx.transform;

import com.zx.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName T4_SplitSelect
 * @Description TODO
 * @Author Xing
 * @Date 11 17:49
 * @Version 1.0
 */
public class T4_SplitSelect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/sensor.txt");

        SingleOutputStreamOperator<SensorReading> map = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fileds = value.split(",");
            return new SensorReading(fileds[0], Long.valueOf(fileds[1]), Double.valueOf(fileds[2]));
        });
//        map.print();
//        分流操作  按照温度30为界分为两条流

//        split  被淘汰了 使用这个
        SingleOutputStreamOperator<SensorReading> process = map.keyBy(SensorReading::getTemperature)
                .process(new KeyedProcessFunction<Double, SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        out.collect(value);
                        if (value.getTemperature() > 30) {
                            ctx.output(new OutputTag<SensorReading>("警告") {
                            }, value);
                        }else {
                            ctx.output(new OutputTag<SensorReading>("正常"){},value);
                        }
                    }
                });
        DataStreamSink<SensorReading> warning = process.getSideOutput(new OutputTag<SensorReading>("警告") {
        }).print("警告");


        DataStreamSink<SensorReading> normal = process.getSideOutput(new OutputTag<SensorReading>("正常") {
        }).print("正常");


//        connect
//        将waring流map成二元组  与低温流合并 输出状态信息







        
        env.execute();
    }
}
