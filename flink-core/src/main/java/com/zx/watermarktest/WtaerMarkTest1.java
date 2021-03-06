package com.zx.watermarktest;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName WtaerMarkTest1
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class WtaerMarkTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//     public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
//		this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
//		if (characteristic == TimeCharacteristic.ProcessingTime) {
//			getConfig().setAutoWatermarkInterval(0);
//		} else {
//			getConfig().setAutoWatermarkInterval(200);
//		}
//	}
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(300);

        env.execute();
    }
}
