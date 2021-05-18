package com.zx.streamingwc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName WordCountStream
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> lineDSS = env.readTextFile("flink-test/input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordOne = lineDSS.flatMap(new MyFlatMap())
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        wordOne.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) {
                return value.f0;
            }
        }).sum(1).print();
        env.execute("javaReadFileWordCount");
    }

    public static class MyFlatMap implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String value, Collector<String> out) {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(word);
            }

        }
    }
}
