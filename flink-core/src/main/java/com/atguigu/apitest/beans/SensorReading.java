package com.atguigu.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName SensorReading
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/10 23:09
 * @Version 1.0
 */
@Data
public class SensorReading {
    private String id;
    private Long timeStamp;
    private Double temperature;

    public SensorReading(String id, Long timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }

    public SensorReading() {
    }
}
