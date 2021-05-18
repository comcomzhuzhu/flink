package com.zx.patternapi;

import com.zx.apitest.beans.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CEP_LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textFile = env.readTextFile("flink-test/input/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> caseClassDS = textFile.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) {
                String[] split = value.split(",");
                return new LoginEvent(split[0],
                        split[1],
                        split[2],
                        Long.valueOf(split[3]));
            }
        });

//        提取watermark
        SingleOutputStreamOperator<LoginEvent> withWMDS = caseClassDS.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

//        分组
        KeyedStream<LoginEvent, String> keyedStream = withWMDS.keyBy(LoginEvent::getId);

//        定义模式序列
//        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent value) {
//                        return "fail".equals(value.getState());
//                    }
//                }).next("next")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent value) {
//                        return "fail".equals(value.getState());
//                    }
//                }).within(Time.seconds(2));

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) {
                        return "fail".equals(value.getState());
                    }
                })
                .times(2)         //默认使用宽松近邻
                .consecutive()    //指定严格近邻
                .within(Time.seconds(2));


//        将模式序列作用流
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

//        提取数据
        SingleOutputStreamOperator<String> selectDS = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) {
                List<LoginEvent> start = map.get("start");
//                List<LoginEvent> next = map.get("next");
                String id = start.get(0).getId();
                Long ts1 = start.get(0).getTs();
                Long ts2 = start.get(1).getTs();
//                Long ts2 = next.get(0).getTs();
                return id + "在" + ts1 + "--" + ts2 + "之间失败两次";
            }
        });
        selectDS.print();



//        selectDS.getSideOutput()

        env.execute();
    }
}
