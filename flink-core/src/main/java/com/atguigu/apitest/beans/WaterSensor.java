package com.atguigu.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName WaterSensor
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/16 14:35
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
