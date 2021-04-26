package com.atguigu.patternapi;

import com.atguigu.apitest.beans.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName TimeOut_CEP
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/20 16:20
 * @Version 1.0
 */
public class TimeOut_CEP {
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

        SingleOutputStreamOperator<String> selectDS =
                patternStream.select(new OutputTag<String>("timeOut") {
                                     },
                        new PatternTimeoutFunction<LoginEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<LoginEvent>> pattern, long timeoutTimestamp) {
                                LoginEvent loginEvent = pattern.get("start").get(0);
                                return loginEvent.getId() + "单次失败" + timeoutTimestamp;
                            }
                        }, new PatternSelectFunction<LoginEvent, String>() {
                            @Override
                            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                                List<LoginEvent> eventList = pattern.get("start");
                                LoginEvent startEvent = eventList.get(0);
                                LoginEvent nextEvent = eventList.get(1);
                                return startEvent.getId() + "在" + startEvent.getTs() + "到" + nextEvent.getTs() + "之间连续失败2次";
                            }
                        });
        selectDS.print("select");
        selectDS.getSideOutput(new OutputTag<String>("timeOut") {
        }).print("side");

        env.execute();
    }
}
