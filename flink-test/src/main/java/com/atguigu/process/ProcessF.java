package com.atguigu.process;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName ProcessF
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/17 10:56
 * @Version 1.0
 */
public class ProcessF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> re0DS = caseClassDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() > 30) {
                    ctx.output(new OutputTag<WaterSensor>("side") {
                    }, value);
                } else {
                    out.collect(value);
                }
            }
        });

        re0DS.print("re");

        re0DS.getSideOutput(new OutputTag<WaterSensor>("side") {
        }).print("side");

        env.execute();
    }


    public static class MyPro extends ProcessFunction<String, String> {

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public void open(Configuration parameters) {
            RuntimeContext runtimeContext = getRuntimeContext();
//            状态编程
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
//          2 主流数据
            out.collect("");

//            process 的强大
//          3 侧输出流
            ctx.output(new OutputTag<String>("xx") {
            }, "");

//          4 定时器
            ctx.timerService().currentWatermark();
            long ts = ctx.timerService().currentProcessingTime();

            ctx.timerService().registerEventTimeTimer(5000);
            ctx.timerService().registerProcessingTimeTimer(ts + 5000);

            ctx.timerService().deleteProcessingTimeTimer(ts + 5000);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
//            定时器 触发 的方法

        }
    }
}
