package com.zx.work;

import com.zx.apitest.beans.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName W1
 * @Description TODO
 * @Author Xing
 * @Date 13 15:00
 * @Version 1.0
 */
public class W1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("flink-test/input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .map(behavior -> Tuple2.of("pv", 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG)) // 使用Tuple类型, 方便后面求和
                .keyBy(value -> value.f0)  // keyBy: 按照key分组
                .sum(1) // 求和
                .print();

        env.execute();

    }
}
