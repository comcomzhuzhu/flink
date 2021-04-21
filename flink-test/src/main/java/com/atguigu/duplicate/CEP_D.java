package com.atguigu.duplicate;

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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName CEP_D
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/20 20:46
 * @Version 1.0
 */
public class CEP_D {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("flink-test/input/LoginLog.csv");

        SingleOutputStreamOperator<LoginEvent> caseClassDS = dataStreamSource.map(new MapFunction<String, LoginEvent>() {
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

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getState());
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(2));


        PatternStream<LoginEvent> patternStream = CEP.pattern(withWMDS, pattern);


        patternStream.select(new OutputTag<String>("side") {
                             },
                new PatternTimeoutFunction<LoginEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<LoginEvent>> pattern, long timeoutTimestamp) throws Exception {
                        LoginEvent loginEvent = pattern.get("start").get(0);
                        return loginEvent.getId() + "单次失败" + timeoutTimestamp;
                    }
                }, new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        List<LoginEvent> loginEvents = pattern.get("start");

                        LoginEvent loginEvent = loginEvents.get(0);
                        LoginEvent endEvent = loginEvents.get(1);

                        return loginEvent.getId() + loginEvent.getTs() + "-" + endEvent.getTs() + "失败两次";
                    }
                }).print();


        env.execute();
    }
}
