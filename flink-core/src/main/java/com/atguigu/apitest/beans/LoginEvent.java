package com.atguigu.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName LoginEvent
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/20 14:30
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private String id;
    private String ip;
    private String state;
    private Long ts;
}
