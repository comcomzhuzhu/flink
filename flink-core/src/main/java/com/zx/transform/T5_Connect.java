package com.zx.transform;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName T5_Connect
 * @Description TODO
 * @Author Xing
 * 11 20:34
 * @Version 1.0
 */
public class T5_Connect {
//     TODO 只能连接两条流
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> intDSS = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> stringDSS = env.fromElements("a", "b", "c", "d");

        ConnectedStreams<Integer, String> cs = intDSS.connect(stringDSS);

        cs.getFirstInput().print("first");

        cs.getSecondInput().print("second");


        SingleOutputStreamOperator<Object> coMap = cs.map(new CoMapFunction<Integer, String, Object>() {
            @Override
            public Object map1(Integer value) {
                return new Tuple2<>(value,"int");
            }
            @Override
            public Object map2(String value) {
                return new Tuple3<>(value,"string","is");
            }
        });
        coMap.print("coMap");

        env.execute();
    }
}
