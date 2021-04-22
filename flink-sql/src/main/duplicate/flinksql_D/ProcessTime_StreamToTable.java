package flinksql_D;

import com.atguigu.apitest.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName ProcessTime_StreamToTable
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/21 20:51
 * @Version 1.0
 */
public class ProcessTime_StreamToTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> caseDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });

        Table table = tableEnvironment.fromDataStream(caseDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        table.printSchema();


        env.execute();
    }
}
