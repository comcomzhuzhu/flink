package com.atguigu.sourcetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @ClassName Source_CusSource
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/11 10:43
 * @Version 1.0
 * <p>
 * <p>
 * main方法运行在jobManager 或者 提交本地
 * main方法中的对象和taskManager的对象不是同一个
 */
public class Source_CusSource {
    private static final Logger logger = LoggerFactory.getLogger(Source_CusSource.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySensorSource mySensorSource = new MySensorSource();
        DataStreamSource<SensorReading> dss = env.addSource(mySensorSource);
        dss.print();
        env.execute();

    }

    public static class MySensorSource implements SourceFunction<SensorReading> {
        //        private AtomicBoolean running = new AtomicBoolean(true);
        boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            logger.info("running?" + running);
            logger.info("run:" + this.hashCode());
            Random r = new Random();
            HashMap<String, Double> tempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                tempMap.put("sensor" + (i + 1), r.nextGaussian() * 10);
            }
            while (running) {
                System.out.println(running);
                for (String key : tempMap.keySet()) {
                    // 当前温度基础上随机波动
                    Double newTemp = tempMap.get(key) + r.nextGaussian();
                    tempMap.put(key, newTemp);
                    ctx.collect(new SensorReading(key, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(1000);
                new Thread(() -> {
                    try {
                        Thread.sleep(5000);
                        this.cancel();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }).start();
            }
        }

        @Override
        public void cancel() {
            running = false;
            System.out.println(running);
        }
    }
}
