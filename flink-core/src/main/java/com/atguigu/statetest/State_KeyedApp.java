package com.atguigu.statetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;


public class State_KeyedApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.socketTextStream("hadoop102", 1208);
        SingleOutputStreamOperator<SensorReading> dataDS = dss.map((MapFunction<String, SensorReading>) value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warningDS = dataDS.keyBy(SensorReading::getId)
                .flatMap(new TempChangeWarning(10.0));





        warningDS.print("state temp");
        env.execute();
    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold;

        //        保存上一次的状态
        private ValueState<Double> lastTemp;

        @Override
        public void open(Configuration parameters) {
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastTemp", Double.class)
            );
        }

        @Override
        public void close() {
            lastTemp.clear();
        }

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }


        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws IOException {
//             获取之前存的状态
            Double lastTempD = lastTemp.value();
            if (lastTempD != null) {
                Double diff = Math.abs(value.getTemperature() - lastTempD);
                if (diff>=threshold){
                    out.collect(Tuple3.of(value.getId(),lastTempD,value.getTemperature()));
                }
            }

//            将当前温度值 保存到上一次温度
            lastTemp.update(value.getTemperature());
        }
    }
}
