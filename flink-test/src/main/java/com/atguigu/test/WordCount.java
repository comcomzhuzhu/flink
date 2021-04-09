package com.atguigu.test;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Collection;


/**
 * @ClassName WordCount
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/8 16:45
 * @Version 1.0
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDS = env.readTextFile("");

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] strings = line.split(" ");
                    for (String word : strings) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        AggregateOperator<Tuple2<String, Long>> sum = wordAndOne.groupBy(0).sum(1);
        sum.print();
    }
}
