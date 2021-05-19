package com.zx.watermarkstrategytest;

import com.zx.apitest.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName CustomPunctuated
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class CustomPunctuated {
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
    }


    public static class MyPunctuated implements WatermarkGenerator<WaterSensor> {
        private Long orderness;
        private Long maxTs;

        public MyPunctuated() {
        }

        public MyPunctuated(Long orderness) {
            this.orderness = orderness;
            maxTs = Long.MIN_VALUE + orderness + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(event.getTs() * 1000L, maxTs);
            output.emitWatermark(new Watermark(maxTs-orderness-1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
