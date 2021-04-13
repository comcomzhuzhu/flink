package com.atguigu.work;

import com.atguigu.apitest.beans.MarketingUserBehavior;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @ClassName W_AppAnalysis_By_Chanel
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/13 15:10
 * @Version 1.0
 */
public class W_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> source = env.addSource(new MarketSource());





        env.execute();
    }


    public static class MarketSource extends RichSourceFunction<MarketingUserBehavior> {
        private volatile boolean running = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");


        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}

