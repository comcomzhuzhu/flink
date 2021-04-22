package com.atguigu.work;

import com.atguigu.apitest.beans.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Work_PV
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/13 14:08
 * @Version 1.0
 */
public class Work_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dss = env.readTextFile("flink-test/input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> caseClassDS = dss.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) {
                String[] strings = value.split(",");
                return new UserBehavior(Long.valueOf(strings[0]),
                        Long.valueOf(strings[1]),
                        Integer.valueOf(strings[2]),
                        strings[3],
                        Long.valueOf(strings[4]));
            }
        });

        caseClassDS.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) {
                return "pv".equals(value.getBehavior());
            }
        }).keyBy((KeySelector<UserBehavior, Object>) UserBehavior::getBehavior)
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) {
                        return Tuple2.of("pv", 1L);
                    }
                });


        env.execute();
    }
}
