package com.zx.sink;

import com.zx.apitest.beans.SensorReading;
import com.zx.sourcetest.Source_CusSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class Sink_Mysql {
    private static Logger logger = LoggerFactory.getLogger(Sink_Mysql.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> textDS = env.readTextFile("flink-test/input/sensor.txt");
//        SingleOutputStreamOperator<SensorReading> caseClassDS = textDS.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) {
//                String[] split = value.split(",");
//                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
//            }
//        });

        DataStreamSource<SensorReading> sourceDS = env.addSource(new Source_CusSource.MySensorSource());
        sourceDS.print();
        sourceDS.addSink(new MyJDBCSink());
        env.execute();
    }

    private static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        Connection conn = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void invoke(SensorReading value, Context context) throws SQLException {
//            每来一条数据 调用连接执行sql  如果没有更新成功 就插入
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            logger.debug(Integer.toString(updateStmt.getUpdateCount()));
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://zx101:3306/test?useSSL=false", "root", "123456");
            insertStmt = conn.prepareStatement("insert into  sensor_temp(id,temp)values (?,?)");
            updateStmt = conn.prepareStatement("update  sensor_temp set temp = ? where id =?");
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            conn.close();
        }
    }
}
