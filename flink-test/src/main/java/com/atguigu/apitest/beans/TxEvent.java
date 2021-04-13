package com.atguigu.apitest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName TxEvent
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/13 15:43
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
