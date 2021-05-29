package flinksql_D;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName SlideWindow
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class SlideWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("zx101", 8888);

        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        Table table = tableEnvironment.fromDataStream(caseClassDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());


        table.window(Tumble.over(lit(5).second()).on($("pt")).as("w"))
                .groupBy($("id"),$("w"))
                .aggregate($("id").count().as("vnt"))
                .select($("id"),$("cnt"))
                .execute()
                .print();

        table.window(Session.withGap(lit(5).second()).on($("pt")).as("w"))
                .groupBy($("id"),$("w"))
                .aggregate($("id").count().as("vnt"))
                .select($("id"),$("cnt"))
                .execute()
                .print();

        table.window(Tumble.over(rowInterval(5L)).on($("pt")).as("w"))
                .groupBy($("id"),$("w"))
                .aggregate($("id").count().as("vnt"))
                .select($("id"),$("cnt"))
                .execute()
                .print();

//        五条数据出发计算 之后每两条出发计算
        table.window(Slide.over(rowInterval(5L)).every(rowInterval(2L))
        .on($("pt")).as("w"));


        env.execute();
    }
}
