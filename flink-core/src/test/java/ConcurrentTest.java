import com.zx.apitest.beans.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @ClassName ConcurrentTest
 * @Description TODO
 * @Author Xing
 * 26 8:25
 * @Version 1.0
 */
public class ConcurrentTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

//        data from socket ass water mark near source  for the last data only need put one time
        SingleOutputStreamOperator<String> withWMDS = socketTextStream.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.valueOf(split[1]) * 1000000L;
                    }
                }));

//        map to POJO
        SingleOutputStreamOperator<WaterSensor> caseClassDS = withWMDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) {
                String[] strings = value.split(",");
                return new WaterSensor(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
            }
        });


//          keyBy and out the top3 of every key in richFlatMapFunc
        SingleOutputStreamOperator<Tuple3<Double, String, Integer>> flatMapDS = caseClassDS.keyBy(WaterSensor::getId)
                .flatMap(new TestFunction(3));


        env.execute();
    }

    public static class TestFunction extends RichFlatMapFunction<WaterSensor, Tuple3<Double, String, Integer>> {

        private int topSize;
        private MapState<Double, Tuple2<String, Integer>> mapState;
        volatile ArrayList<Double> top3 = new ArrayList<>();

        @Override
        public void open(Configuration parameters) {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map",
                    TypeInformation.of(Double.class),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                    })
            ));
        }

        public TestFunction(Integer topSize) {
            this.topSize = topSize;
        }
        @Override
        public void flatMap(WaterSensor value, Collector<Tuple3<Double, String, Integer>> out) throws Exception {
            if (mapState.contains(value.getVc())) {
                Tuple2<String, Integer> tuple2 = mapState.get(value.getVc());
                mapState.put(value.getVc(), Tuple2.of(value.getId(), tuple2.f1 + 1));
            } else {
                mapState.put(value.getVc(), Tuple2.of(value.getId(), 1));
            }

            ArrayList<Double> keys = Lists.newArrayList(mapState.keys().iterator());

            keys.sort(((o1, o2) -> -o1.compareTo(o2)));


            if (keys.size() >= 3) {
                for (int i = 0; i < 3; i++) {
                    top3.add(keys.get(i));
                }
            }


            if (top3.size() >= 3) {
                for (Double aDouble : mapState.keys()) {
                    if (!top3.contains(aDouble)) {
                        mapState.remove(aDouble);
                    }
                }
            }


            System.out.println(keys);

            for (int i = 0; i < Math.min(keys.size(), 3); i++) {
                Double aDouble = keys.get(i);
                Tuple2<String, Integer> tuple2 = mapState.get(aDouble);
                out.collect(Tuple3.of(aDouble, tuple2.f0, tuple2.f1));
            }

        }
    }
}