package com.zx.work;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName SensorCount
 * @Description TODO
 * @Author Xing
 * 2021/5/12 15:00
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorCount {
    private Long stt;
    private Long edt;
    private String id;
    private Integer count;
}