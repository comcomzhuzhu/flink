package com.atguigu.apitest.sourcetest;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class Source_CustomSourceSocket {
    private static final Logger logger = LoggerFactory.getLogger(Source_CustomSourceSocket.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySource source = new MySource("hadoop102", 9997);
        DataStreamSource<SensorReading> dss = env.addSource(source);
        dss.print();
        logger.info("main"+source.hashCode());
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("thread:"+source.hashCode());
//                source.cancel();
        }).start();
        env.execute();
    }


    public static class MySource implements SourceFunction<SensorReading> {
        private String host;
        private int port;
        private volatile boolean isRunning = true;
        private Socket socket;
        private AtomicBoolean is = new AtomicBoolean(true);


        public MySource(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            logger.info("run:"+this.hashCode());
            socket = new Socket(host, port);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String line;
            while (isRunning && (line = reader.readLine()) != null) {
                System.out.println(line);
                String[] split = line.split(",");
                ctx.collect(new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String toString() {
            return "MySource{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", isRunning=" + isRunning +
                    ", socket=" + socket +
                    '}';
        }
    }
}
