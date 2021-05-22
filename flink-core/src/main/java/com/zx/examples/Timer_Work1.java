package com.zx.examples;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class Timer_Work1 {
    public static void main(String[] args) {
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

        KeyedStream<WaterSensor, String> keyedStream = caseClassDS.keyBy(WaterSensor::getId);
        SingleOutputStreamOperator<WaterSensor> processDS = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private Double lastVc = Double.MIN_VALUE;
            private Long timerTs = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                out.collect(value);
                TimerService timerService = ctx.timerService();
                Double vc = value.getVc();

////                如果是第一套数据  注册5S以后的定时器
//                if (lastVc == Double.MIN_VALUE) {
////                   注册定时器
//                    long ts = timerService.currentProcessingTime();
//                    timerTs = ts + 5000L;
//                    timerService.registerProcessingTimeTimer(timerTs);
////                    替换上一次的值
//                    lastVc = value.getVc();
//                } else
                if (vc < lastVc) {  //水位线下降
//                    删除定时器
                    timerService.deleteProcessingTimeTimer(timerTs);
//                    修改上一次水位线 为本数据的
                    lastVc = vc;
//                    删除之后 重置时间戳
                    timerTs = Long.MIN_VALUE;
                } else if (vc > lastVc && timerTs == Long.MIN_VALUE) {
//                    注册定时器
                    long ts = timerService.currentProcessingTime();
                    timerTs = ts + 5000L;
                    timerService.registerProcessingTimeTimer(timerTs);

//                    替换上一次的值
                    lastVc = value.getVc();
                }


            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("side"){},
                        ctx.getCurrentKey() + "传感器连续5S水位没有下降");
                timerTs = Long.MIN_VALUE;
            }
        });


        processDS.print("result");

        processDS.getSideOutput(new OutputTag<String>("side"){}).print("side");


    }
}
