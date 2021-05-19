package flinksql_D;


import com.zx.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName DS_Agg
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class DS_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<WaterSensor> caseClassDS = socketTextStream.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        tableEnvironment.createTemporaryView("sensor", caseClassDS);
        Table table = tableEnvironment.sqlQuery("select id,count(id) from sensor group by id");

        DataStream<Tuple2<Boolean, Row>> resultDS = tableEnvironment.toRetractStream(table, Row.class);

        resultDS.print();
        env.execute();
    }
}
