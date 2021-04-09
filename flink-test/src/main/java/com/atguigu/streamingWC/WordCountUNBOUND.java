package com.atguigu.streamingWC;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ClassName WordCountUNBOUND
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/8 17:08
 * @Version 1.0
 */
public class WordCountUNBOUND {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lineDDS = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDDS.flatMap((String line, Collector<String> words) -> {
            Arrays.stream(line.split(" ")).forEach(words::collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        wordAndOne.keyBy(t->t.f0).sum(1).print();
        env.execute();
    }
}
