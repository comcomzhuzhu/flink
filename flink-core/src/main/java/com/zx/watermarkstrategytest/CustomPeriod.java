package com.zx.watermarkstrategytest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName CustomPeriod
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class CustomPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 7777);
        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        SingleOutputStreamOperator<WaterSensor> withWMDS = caseClassDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod();
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));


        withWMDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .sum("vc")
                .print("");
        env.execute();
    }


    public static class MyPeriod implements WatermarkGenerator<WaterSensor> {

        private Long orderness;
        private Long maxTs;

        public MyPeriod() {
        }

        public MyPeriod(Long orderness) {
            this.orderness = orderness;
            maxTs = Long.MIN_VALUE + orderness + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
//            ?????????????????? ????????????????????????????????????????????? ??????watermark????????????
            maxTs = Math.max(event.getTs() * 1000L, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTs - orderness - 1));
        }
    }
}
