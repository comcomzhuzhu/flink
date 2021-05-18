package com.zx.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Double vc;
}
