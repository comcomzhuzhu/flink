package com.atguigu.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textDS = env.readTextFile("flink-test/input/sensor.txt");
        SingleOutputStreamOperator<SensorReading> caseClassDS = textDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });
        List<HttpHost> httpHosts = new ArrayList<>();

        httpHosts.add(new HttpHost("hadoop102",9200,"http"));
        httpHosts.add(new HttpHost("hadoop103",9200,"http"));
        httpHosts.add(new HttpHost("hadoop104",9200,"http"));

        ElasticsearchSink<SensorReading> build = new ElasticsearchSink.Builder<SensorReading>(
                httpHosts,
                new MyEsSinkFun()).build();

        caseClassDS.addSink(build);

        env.execute();
    }

    public static class MyEsSinkFun implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            HashMap<String, String> data = new HashMap<>();
//            定义写入数据
            data.put("id", sensorReading.getId());
            data.put("ts", sensorReading.getTimeStamp().toString());
            data.put("temp", sensorReading.getTemperature().toString());

            IndexRequest request = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .id(sensorReading.getId())
                    .source(data);

            requestIndexer.add(request);
        }

//        创建index 请求

    }
}
