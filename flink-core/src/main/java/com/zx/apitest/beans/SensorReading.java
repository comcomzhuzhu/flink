package com.zx.apitest.beans;

import lombok.Data;


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
