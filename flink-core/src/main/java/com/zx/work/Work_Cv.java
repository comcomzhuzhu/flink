package com.zx.work;

import com.zx.apitest.beans.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName Work_Cv
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class Work_Cv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textFile = env.readTextFile("flink-test/input/UserBehavior.csv");

        textFile.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) {
                return "pv".equals(value.split(",")[3]);
            }
        }).map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) {
                String[] strings = value.split(",");
                return new UserBehavior();
            }
        });



        env.execute();
    }
}
