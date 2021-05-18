package com.zx.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName WaterSensor
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Double vc;
}
